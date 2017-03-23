module Slate.Command.Locker
    exposing
        ( Config
        , Model
        , Msg
        , init
        , update
        , lock
        )

import Dict exposing (..)
import Json.Decode as JD exposing (..)
import FNV exposing (..)
import Postgres exposing (..)
import Utils.Ops exposing (..)
import Utils.Json exposing ((<||))
import Utils.Error exposing (..)
import Utils.Log exposing (..)
import StringUtils exposing (..)
import List exposing (isEmpty)
import Slate.Common.Taggers exposing (..)
import Slate.Command.Common.Command exposing (..)


type alias LockState =
    { entityIds : List String
    , locks : List Int
    }


type alias LockDict =
    Dict ConnectionId LockState


type alias LockResponse =
    { pg_try_advisory_xact_lock : Bool
    }


{-|
parent msg taggers
-}
type alias RouteToMeTagger msg =
    Msg -> msg


type alias LockEntitiesSuccessTagger msg =
    CommandId -> msg


type alias LockEntitiesErrorTagger msg =
    ( CommandId, String ) -> msg


type alias Config msg =
    { retries : Int
    , routeToMeTagger : RouteToMeTagger msg
    , errorTagger : ErrorTagger ( CommandId, String ) msg
    , logTagger : LogTagger ( CommandId, String ) msg
    , lockEntitiesSuccessTagger : LockEntitiesSuccessTagger msg
    , lockEntitiesErrorTagger : LockEntitiesErrorTagger msg
    }


type Msg
    = Nop
    | BeginCommand CommandId Int ( ConnectionId, List String )
    | BeginCommandError CommandId ( ConnectionId, String )
    | LockEntities CommandId Int ( ConnectionId, List String )
    | LockEntitiesError CommandId ( ConnectionId, String )
    | Rollback CommandId Int ( ConnectionId, List String )
    | RollbackError CommandId ( ConnectionId, String )


type alias Model =
    { lockRequests : LockDict
    }


init : (Msg -> msg) -> ( Model, Cmd msg )
init tagger =
    ({ lockRequests = Dict.empty } ! [])


beginTrans : CommandId -> ConnectionId -> Int -> Cmd Msg
beginTrans commandId connectionId retryCount =
    Postgres.query (BeginCommandError commandId) (BeginCommand commandId retryCount) connectionId "BEGIN" 1


lockResponseDecoder : JD.Decoder LockResponse
lockResponseDecoder =
    JD.succeed LockResponse
        <|| (field "pg_try_advisory_xact_lock" bool)


update : Config msg -> Msg -> Model -> ( ( Model, Cmd Msg ), List msg )
update config msg model =
    let
        logMsg commandId message =
            config.logTagger ( LogLevelInfo, ( commandId, message ) )

        nonFatal commandId error =
            config.errorTagger ( NonFatalError, ( commandId, error ) )

        fatal commandId error =
            config.errorTagger ( FatalError, ( commandId, error ) )
    in
        case msg of
            Nop ->
                ( model ! [], [] )

            BeginCommand commandId retryCount ( connectionId, results ) ->
                let
                    fatalParentMsgs =
                        (results == [])
                            ?! ( (\_ -> [])
                               , (\_ -> [ fatal commandId ("SQL BEGIN Command results list is not empty. Results:" ++ (toString results)) ])
                               )
                in
                    (fatalParentMsgs /= [])
                        ? ( ( model ! [], fatalParentMsgs )
                          , processNextLock config model commandId connectionId retryCount
                          )

            BeginCommandError commandId ( connectionId, error ) ->
                ( model ! [], [ nonFatal commandId ("BeginCommandError:" +-+ "ConnectionId:" +-+ connectionId +-+ "SQL error:" +-+ error) ] )

            LockEntities commandId retryCount ( connectionId, results ) ->
                let
                    ( gotLock, fatalParentMsgs ) =
                        didLock config results commandId
                in
                    case fatalParentMsgs /= [] of
                        True ->
                            ( model ! [], fatalParentMsgs )

                        False ->
                            gotLock ? ( processNextLock config model commandId connectionId retryCount, retryLocks config model commandId connectionId retryCount )

            LockEntitiesError commandId ( connectionId, error ) ->
                let
                    errMsg =
                        "Locker LockEntitiesError:" +-+ "ConnectionId:" +-+ connectionId +-+ "SQL error:" +-+ error
                in
                    ( model ! []
                    , [ nonFatal commandId errMsg
                      , config.lockEntitiesErrorTagger ( commandId, errMsg )
                      ]
                    )

            Rollback commandId retryCount ( connectionId, results ) ->
                let
                    fatalParentMsgs =
                        (results == [])
                            ?! ( (\_ -> [])
                               , (\_ -> [ fatal commandId ("SQL ROLLBACK Command results list is not empty. Results:" ++ (toString results)) ])
                               )

                    ( cmd, parentMsgs ) =
                        (retryCount <= config.retries)
                            ? ( ( beginTrans commandId connectionId (retryCount + 1)
                                , [ logMsg commandId ("lock Command Error for commandId:" +-+ commandId +-+ "Error:" +-+ "Could not obtains all locks:" +-+ (Dict.get connectionId model.lockRequests) +-+ "for connectionId:" +-+ connectionId +-+ "Retry:" +-+ retryCount) ]
                                )
                              , ( Cmd.none
                                , [ config.lockEntitiesErrorTagger ( commandId, "Failed to obtain all locks" ) ]
                                )
                              )
                in
                    (fatalParentMsgs /= [])
                        ? ( ( model ! [], fatalParentMsgs )
                          , ( model ! [ cmd ], parentMsgs )
                          )

            RollbackError commandId ( connectionId, error ) ->
                let
                    parentMsgs =
                        [ nonFatal commandId ("Locker RollbackError :" +-+ "ConnectionId:" +-+ connectionId +-+ "SQL error:" +-+ error) ]
                in
                    ( model ! [], parentMsgs )



-- API


lock : Config msg -> Model -> CommandId -> ConnectionId -> List String -> ( Model, Cmd msg )
lock config model commandId connectionId entityIds =
    let
        locks =
            createLocks entityIds

        lockRequests =
            Dict.insert connectionId (LockState entityIds locks) model.lockRequests
    in
        ( { model | lockRequests = lockRequests }
        , Cmd.map config.routeToMeTagger <| beginTrans commandId connectionId 1
        )



-- PRIVATE API


createLocks : List String -> List Int
createLocks entityIds =
    List.map (\guid -> FNV.hashString guid) entityIds


processNextLock : Config msg -> Model -> CommandId -> ConnectionId -> Int -> ( ( Model, Cmd Msg ), List msg )
processNextLock config model commandId connectionId retryCount =
    Dict.get connectionId model.lockRequests
        |?> (\lockState ->
                List.head lockState.locks
                    |?> (\lock ->
                            ( { model | lockRequests = Dict.insert connectionId { lockState | locks = List.drop 1 lockState.locks } model.lockRequests }
                                ! [ lockCmd commandId connectionId lock retryCount ]
                            , []
                            )
                        )
                    ?= ( { model | lockRequests = Dict.remove connectionId model.lockRequests } ! [], [ config.lockEntitiesSuccessTagger commandId ] )
            )
        ?!= (\_ -> ( model ! [], [ config.errorTagger ( FatalError, ( commandId, ("BUG -- Missing connectionId") ) ) ] ))


retryLocks : Config msg -> Model -> CommandId -> ConnectionId -> Int -> ( ( Model, Cmd Msg ), List msg )
retryLocks config model commandId connectionId retryCount =
    Dict.get connectionId model.lockRequests
        |?> (\lockState ->
                ( { model | lockRequests = Dict.insert connectionId { lockState | locks = createLocks lockState.entityIds } model.lockRequests }
                    ! [ Postgres.query (RollbackError commandId) (Rollback commandId retryCount) connectionId "ROLLBACK" 1 ]
                , []
                )
            )
        ?!= (\_ -> ( model ! [], [ config.errorTagger ( FatalError, ( commandId, ("BUG -- Missing connectionId") ) ) ] ))


lockCmd : CommandId -> ConnectionId -> Int -> Int -> Cmd Msg
lockCmd commandId connectionId lock retryCount =
    let
        createLockStatement lock =
            "SELECT pg_try_advisory_xact_lock(" ++ (toString lock) ++ ");"
    in
        Postgres.query (LockEntitiesError commandId) (LockEntities commandId retryCount) connectionId (createLockStatement lock) 2


didLock : Config msg -> List String -> CommandId -> ( Bool, List msg )
didLock config results commandId =
    let
        fatal commandId error =
            config.errorTagger ( FatalError, ( commandId, error ) )
    in
        List.head results
            |?> (\result ->
                    JD.decodeString lockResponseDecoder result
                        |??> (\response -> ( response.pg_try_advisory_xact_lock, [] ))
                        ??= (\err -> ( False, [ fatal commandId ("SQL Lock Command results could not be decoded. Error:" +-+ err) ] ))
                )
            ?!= (\_ -> ( False, [ fatal commandId ("SQL Lock Command results list is empty") ] ))
