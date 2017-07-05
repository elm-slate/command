module Slate.Command.Locker
    exposing
        ( Config
        , Model
        , Msg
        , init
        , update
        , lock
        )

import Time exposing (Time)
import Task
import Process
import Dict exposing (..)
import Json.Decode as JD exposing (..)
import FNV exposing (..)
import Postgres exposing (ConnectionId, QueryTagger, Sql)
import Utils.Ops exposing (..)
import Utils.Json exposing ((<||))
import Utils.Error exposing (..)
import Utils.Log exposing (..)
import StringUtils exposing (..)
import List exposing (isEmpty)
import Slate.Common.Taggers exposing (..)
import Slate.Command.Common.Command exposing (..)


{-| starting delay (in milliseconds) before retrying so retries don't happen too fast (Server ONLY)
-}
startingRetryDelay : Time
startingRetryDelay =
    100


{-| max delay (in milliseconds) before retrying (Server ONLY)
-}
maxRetryDelay : Time
maxRetryDelay =
    1000


type alias LockState =
    { entityIds : List String
    , locks : List Int
    , retryDelay : Time
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
    , debug : Bool
    }


type Msg
    = Nop
    | BeginCommand CommandId Int ( ConnectionId, List String )
    | BeginCommandError CommandId ( ConnectionId, String )
    | LockEntities CommandId Int ( ConnectionId, List String )
    | LockEntitiesError CommandId ( ConnectionId, String )
    | Retry CommandId ConnectionId Int
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
    query (BeginCommandError commandId) (BeginCommand commandId retryCount) connectionId "BEGIN" 1


lockResponseDecoder : JD.Decoder LockResponse
lockResponseDecoder =
    JD.succeed LockResponse
        <|| (field "pg_try_advisory_xact_lock" bool)


delayMsgToCmd : Time -> Msg -> Cmd Msg
delayMsgToCmd delay msg =
    Task.perform (\_ -> msg) <| Process.sleep delay


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

            Retry commandId connectionId retryCount ->
                ( model ! [ beginTrans commandId connectionId retryCount ], [] )

            Rollback commandId retryCount ( connectionId, results ) ->
                (results /= [])
                    ? ( ( model ! []
                        , [ fatal commandId ("SQL ROLLBACK Command results list is not empty. Results:" +-+ results) ]
                        )
                      , Dict.get connectionId model.lockRequests
                            ?!= (\_ -> Debug.crash ("BUG: Cannot find lockState for connectionId:" +-+ connectionId))
                            |> (\lockState ->
                                    (retryCount <= config.retries)
                                        ? ( { model | lockRequests = Dict.insert connectionId { lockState | retryDelay = max (2 * lockState.retryDelay) maxRetryDelay } model.lockRequests }
                                                |> (\model ->
                                                        ( model ! [ delayMsgToCmd (Postgres.isOnClient ? ( 0, lockState.retryDelay )) <| Retry commandId connectionId (retryCount + 1) ]
                                                        , [ logMsg commandId ("lock Command Error for commandId:" +-+ commandId +-+ "Error:" +-+ "Could not obtains all locks:" +-+ lockState +-+ "for connectionId:" +-+ connectionId +-+ "Retry:" +-+ retryCount) ]
                                                        )
                                                   )
                                          , ( model ! [], [ config.lockEntitiesErrorTagger ( commandId, "Failed to obtain all locks" ) ] )
                                          )
                               )
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
            Dict.insert connectionId (LockState entityIds locks startingRetryDelay) model.lockRequests
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
                    ! [ query (RollbackError commandId) (Rollback commandId retryCount) connectionId "ROLLBACK" 1 ]
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
        query (LockEntitiesError commandId) (LockEntities commandId retryCount) connectionId (createLockStatement lock) 2


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


query : Postgres.ErrorTagger msg -> QueryTagger msg -> ConnectionId -> Sql -> Int -> Cmd msg
query errorTagger queryTagger connectionId sql =
    Postgres.query errorTagger queryTagger connectionId ("-- Locker:: ConnectionId:" +-+ connectionId ++ "\n" ++ sql)
