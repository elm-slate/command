module Slate.Command.Helper
    exposing
        ( EntityLockAndValidation(..)
        , Msg
        , Model
        , Config
        , init
        , update
        , initCommand
        , lockAndValidateEntities
        , writeEvents
        , commit
        , rollback
        )

{-|
    Helper functions for writing Slate Entity APIs.

@docs Msg , EntityLockAndValidation, Model , Config, init , update , initCommand , lockAndValidateEntities , writeEvents , commit , rollback
-}

import Set
import Time exposing (Time)
import Dict exposing (Dict)
import Json.Decode as JD exposing (..)
import String exposing (join)
import StringUtils exposing ((+-+), (+++))
import Slate.Common.Entity exposing (..)
import Slate.Common.Schema exposing (..)
import Slate.Command.Locker as Locker exposing (..)
import Slate.Command.Validator as Validator exposing (..)
import Slate.Command.Common.Command exposing (..)
import Slate.Command.Common.Validation exposing (..)
import Slate.Common.Db exposing (..)
import Slate.Common.Event exposing (..)
import Slate.Common.Taggers exposing (..)
import Postgres exposing (ConnectionId, QueryTagger, Sql)
import ParentChildUpdate exposing (..)
import Utils.Json exposing ((<||))
import Utils.Ops exposing (..)
import Utils.Error exposing (..)
import Utils.Log exposing (..)
import Retry exposing (..)


{-|
    Lock and Validation type.
-}
type EntityLockAndValidation
    = LockAndValidate ValidationEvent
    | LockOnly EntityId


type alias CommandState =
    { connectionId : ConnectionId
    , entityLockAndValidations : List EntityLockAndValidation
    }


type alias CommandStateDict =
    Dict CommandId CommandState


type alias InsertEventsResponse =
    { insert_events : Int
    }


{-|
    parent msg taggers
-}
type alias RouteToMeTagger msg =
    Msg -> msg


{-|
    Tagger for parent indicating initCommand succeeded.
-}
type alias InitCommandTagger msg =
    CommandId -> msg


{-|
    Tagger for parent indicating initCommand had error.
-}
type alias InitCommandErrorTagger msg =
    ( CommandId, String ) -> msg


{-|
    Tagger for parent indicating lockAndValidateEntities succeeded.
-}
type alias LockAndValidateSuccessTagger msg =
    CommandId -> msg


{-|
    Tagger for parent indicating lockAndValidateEntities had a LOCK error.
-}
type alias LockErrorTagger msg =
    ( CommandId, String ) -> msg


{-|
    Tagger for parent indicating writeEvents succeeded.
-}
type alias WriteEventsTagger msg =
    ( CommandId, Int ) -> msg


{-|
    Tagger for parent indicating writeEvents had error.
-}
type alias WriteEventsErrorTagger msg =
    ( CommandId, String ) -> msg


{-|
    Tagger for parent indicating commit succeeded.
-}
type alias CommitTagger msg =
    CommandId -> msg


{-|
    Tagger for parent indicating commit had error.
-}
type alias CommitErrorTagger msg =
    ( CommandId, String ) -> msg


{-|
    Tagger for parent indicating rollback succeeded.
-}
type alias RollbackTagger msg =
    CommandId -> msg


{-|
    Tagger for parent indicating rollback had error.
-}
type alias RollbackErrorTagger msg =
    ( CommandId, String ) -> msg


{-|
    Tagger for parent indicating database connection was closed unexpectedly.
-}
type alias ConnectionLostTagger msg =
    ( CommandId, String ) -> msg


{-|
    CommandHelper Config.
-}
type alias Config msg =
    { retryMax : Maybe Int
    , delayNext : Maybe (Int -> Time)
    , lockRetries : Maybe Int
    , routeToMeTagger : RouteToMeTagger msg
    , errorTagger : ErrorTagger ( CommandId, String ) msg
    , logTagger : LogTagger ( CommandId, String ) msg
    , initCommandTagger : InitCommandTagger msg
    , initCommandErrorTagger : InitCommandErrorTagger msg
    , lockAndValidateSuccessTagger : LockAndValidateSuccessTagger msg
    , lockErrorTagger : LockErrorTagger msg
    , validateErrorTagger : EntityValidationErrorTagger msg
    , writeEventsTagger : WriteEventsTagger msg
    , writeEventsErrorTagger : WriteEventsErrorTagger msg
    , commitTagger : CommitTagger msg
    , commitErrorTagger : CommitErrorTagger msg
    , rollbackTagger : RollbackTagger msg
    , rollbackErrorTagger : RollbackErrorTagger msg
    , connectionLostTagger : ConnectionLostTagger msg
    , schemaDict : Dict EntityName EntitySchema
    , queryBatchSize : Maybe Int
    , debug : Bool
    }


lockerConfig : Config msg -> Locker.Config Msg
lockerConfig config =
    { retries = config.lockRetries ?= 10
    , routeToMeTagger = LockerMsg
    , errorTagger = LockerError
    , logTagger = LockerLog
    , lockEntitiesSuccessTagger = LockEntitiesSuccess
    , lockEntitiesErrorTagger = LockEntitiesError
    , debug = config.debug
    }


validatorConfig : Config msg -> Validator.Config Msg
validatorConfig config =
    { routeToMeTagger = ValidatorMsg
    , errorTagger = ValidatorError
    , logTagger = ValidatorLog
    , validateEntitiesSuccessTagger = ValidationEntitiesSuccess
    , validateEntitiesErrorTagger = ValidationEntitiesError
    , schemaDict = config.schemaDict
    , queryBatchSize = config.queryBatchSize
    , debug = config.debug
    }


retryConfig : Config msg -> CommandId -> Retry.Config Msg
retryConfig config commandId =
    { retryMax = config.retryMax ?= 10
    , delayNext = config.delayNext ?= Retry.constantDelay 5000
    , routeToMeTagger = RetryMsg commandId
    }


{-|
    CommandHelper Msgs
-}
type Msg
    = Nop
    | PGConnect CommandId ConnectionId
    | PGConnectError CommandId ( ConnectionId, String )
    | PGConnectionLost CommandId ( ConnectionId, String )
    | PGDisconnectError CommandId ( ConnectionId, String )
    | PGDisconnect CommandId ConnectionId
    | LockEntitiesSuccess CommandId
    | LockEntitiesError ( CommandId, String )
    | ValidationEntitiesSuccess CommandId
    | ValidationEntitiesError ( CommandId, List ( Event, String ) )
    | Begin CommandId String ( ConnectionId, List String )
    | BeginError CommandId String ( ConnectionId, String )
    | Commit CommandId ( ConnectionId, List String )
    | CommitError CommandId ( ConnectionId, String )
    | Rollback CommandId ( ConnectionId, List String )
    | RollbackError CommandId ( ConnectionId, String )
    | WriteEvents CommandId String ( ConnectionId, List String )
    | WriteEventsError CommandId String ( ConnectionId, String )
    | LockerError ( ErrorType, ( CommandId, String ) )
    | LockerLog ( LogLevel, ( CommandId, String ) )
    | LockerMsg Locker.Msg
    | ValidatorError ( ErrorType, ( CommandId, String ) )
    | ValidatorLog ( LogLevel, ( CommandId, String ) )
    | ValidatorMsg (Validator.Msg Msg)
    | RetryConnectCmd Int Msg (Cmd Msg)
    | RetryMsg CommandId (Retry.Msg Msg)


{-|
    CommandHelper Config.
-}
type alias Model =
    { commandStates : CommandStateDict
    , nextCommandId : CommandId
    , lockerModel : Locker.Model
    , validatorModel : Validator.Model
    , retryModels : Dict CommandId (Retry.Model Msg)
    }


initModel : ( Model, List (Cmd Msg) )
initModel =
    let
        ( lockerModel, lockerCmd ) =
            Locker.init LockerMsg

        ( validatorModel, validatorCmd ) =
            Validator.init ValidatorMsg
    in
        ( { commandStates = Dict.empty
          , nextCommandId = 0
          , lockerModel = lockerModel
          , validatorModel = validatorModel
          , retryModels = Dict.empty
          }
        , [ lockerCmd, validatorCmd ]
        )


{-|
    initialize command helper
-}
init : Config msg -> ( Model, Cmd msg )
init config =
    let
        ( model, cmds ) =
            initModel
    in
        model ! (List.map (Cmd.map config.routeToMeTagger) cmds)


insertEventsResponseDecoder : JD.Decoder InsertEventsResponse
insertEventsResponseDecoder =
    JD.succeed InsertEventsResponse
        <|| (field "insert_events" int)


{-|
    update
-}
update : Config msg -> Msg -> Model -> ( ( Model, Cmd Msg ), List msg )
update config msg model =
    let
        removeCommandState commandId model =
            { model | commandStates = Dict.remove commandId model.commandStates }

        logMsg commandId message =
            config.logTagger ( LogLevelInfo, ( commandId, message ) )

        debugMsg commandId message =
            config.logTagger ( LogLevelDebug, ( commandId, message ) )

        nonFatal commandId error =
            config.errorTagger ( NonFatalError, ( commandId, error ) )

        fatal commandId error =
            config.errorTagger ( FatalError, ( commandId, error ) )

        childError module_ ( errorType, ( commandId, message ) ) =
            let
                errorHandler =
                    case errorType of
                        FatalError ->
                            fatal

                        NonFatalError ->
                            nonFatal

                        _ ->
                            Debug.crash "Unexpected error type"
            in
                ( model ! [], [ errorHandler commandId (module_ ++ ":" +-+ message) ] )

        childLog module_ ( logLevel, ( commandId, message ) ) =
            let
                logHandler =
                    case logLevel of
                        LogLevelInfo ->
                            logMsg

                        LogLevelDebug ->
                            debugMsg

                        _ ->
                            Debug.crash ("Unexpected log level:" +-+ logLevel)
            in
                ( model ! [], [ logHandler commandId (module_ ++ ":" +-+ message) ] )

        disconnectCommon returnToPool commandId connectionId =
            Postgres.disconnect (PGDisconnectError commandId) (PGDisconnect commandId) connectionId False

        errorDisconnect =
            disconnectCommon False

        disconnect =
            disconnectCommon True

        updateLocker =
            ParentChildUpdate.updateChildParent (Locker.update <| lockerConfig config) (update config) .lockerModel LockerMsg (\model lockerModel -> { model | lockerModel = lockerModel })

        updateValidator =
            ParentChildUpdate.updateChildParent (Validator.update (validatorConfig config)) (update config) .validatorModel ValidatorMsg (\model validatorModel -> { model | validatorModel = validatorModel })

        getRetryModel config model commandId =
            Dict.get commandId model.retryModels
                ?!= (\_ -> Debug.crash ("BUG: Cannot find retry model for commandId:" +-+ commandId))

        updateRetry commandId =
            retryConfig config commandId
                |> (\retryConfig -> ParentChildUpdate.updateChildParent (Retry.update retryConfig) (update config) (\model -> getRetryModel config model commandId) retryConfig.routeToMeTagger (\model retryModel -> { model | retryModels = Dict.insert commandId retryModel model.retryModels }))
    in
        case msg of
            Nop ->
                ( model ! [], [] )

            PGConnect commandId connectionId ->
                let
                    commandStates =
                        Dict.insert commandId (CommandState connectionId []) model.commandStates
                in
                    { model | retryModels = Dict.remove commandId model.retryModels }
                        |> (\model ->
                                ( { model | commandStates = commandStates } ! []
                                , [ debugMsg commandId ("PGConnect:" +-+ "Connection Id:" +-+ connectionId)
                                  , config.initCommandTagger commandId
                                  ]
                                )
                           )

            PGConnectError commandId ( _, error ) ->
                { model | retryModels = Dict.remove commandId model.retryModels }
                    |> (\model -> ( model ! [], [ config.initCommandErrorTagger ( commandId, error ) ] ))

            PGConnectionLost commandId ( connectionId, error ) ->
                ( removeCommandState commandId model ! []
                , [ config.connectionLostTagger ( commandId, "PGConnectLost:" +-+ "Connection Id:" +-+ connectionId +-+ "Connection Error:" +-+ error ) ]
                )

            PGDisconnectError commandId ( connectionId, error ) ->
                let
                    parentMsgs =
                        [ nonFatal commandId ("PGDisconnectError:" +-+ "Connection Id:" +-+ connectionId +-+ "Connection Error:" +-+ error) ]
                in
                    ( removeCommandState commandId model ! [], parentMsgs )

            PGDisconnect commandId connectionId ->
                ( removeCommandState commandId model ! [], [] )

            LockEntitiesSuccess commandId ->
                let
                    commandState =
                        Dict.get commandId model.commandStates ?!= (\_ -> Debug.crash ("Cannot find commandId:" +-+ commandId))

                    entityValidations =
                        commandState.entityLockAndValidations
                            |> List.foldr
                                (\entityLockAndValidation validations ->
                                    case entityLockAndValidation of
                                        LockAndValidate validationEvent ->
                                            validationEvent :: validations

                                        LockOnly _ ->
                                            validations
                                )
                                []

                    ( validatorModel, cmd ) =
                        Validator.validate (validatorConfig config) model.validatorModel commandId commandState.connectionId entityValidations
                in
                    ( { model | validatorModel = validatorModel } ! [ cmd ], [] )

            LockEntitiesError ( commandId, error ) ->
                ( model ! [], [ config.lockErrorTagger ( commandId, error ) ] )

            ValidationEntitiesSuccess commandId ->
                ( model ! [], [ config.lockAndValidateSuccessTagger commandId ] )

            ValidationEntitiesError ( commandId, errors ) ->
                ( model ! [], [ config.validateErrorTagger ( commandId, errors ) ] )

            Begin commandId statement ( connectionId, results ) ->
                let
                    cmd =
                        query commandId (WriteEventsError commandId statement) (WriteEvents commandId statement) connectionId statement 2
                in
                    ( model ! [ cmd ], [] )

            BeginError commandId statement ( connectionId, error ) ->
                ( model ! [], [ config.writeEventsErrorTagger ( commandId, error ) ] )

            Commit commandId ( connectionId, results ) ->
                ( model ! [ disconnect commandId connectionId ], [ config.commitTagger commandId ] )

            CommitError commandId ( connectionId, error ) ->
                ( model ! [ errorDisconnect commandId connectionId ], [ config.commitErrorTagger ( commandId, error ) ] )

            Rollback commandId ( connectionId, results ) ->
                ( model ! [ disconnect commandId connectionId ], [ config.rollbackTagger commandId ] )

            RollbackError commandId ( connectionId, error ) ->
                ( model ! [ errorDisconnect commandId connectionId ], [ config.rollbackErrorTagger ( commandId, error ) ] )

            WriteEvents commandId statement ( connectionId, results ) ->
                let
                    ( eventRows, errorMsg ) =
                        List.head results
                            |?> (\result ->
                                    JD.decodeString insertEventsResponseDecoder result
                                        |??> (\response -> ( response.insert_events, "" ))
                                        ??= (\err -> ( -1, "SQL insert_events Command results could not be decoded. Error:" +-+ err ))
                                )
                            ?!= (\_ -> ( -1, "SQL Insert Events Command results list is EMPTY" ))
                in
                    ( model ! []
                    , [ (errorMsg /= "") ? ( fatal commandId errorMsg, config.writeEventsTagger ( commandId, eventRows ) )
                      ]
                    )

            WriteEventsError commandId statement ( connectionId, error ) ->
                ( model ! [], [ config.writeEventsErrorTagger ( commandId, error ) ] )

            LockerError error ->
                childError "Locker" error

            LockerLog logInfo ->
                childLog "Locker" logInfo

            LockerMsg msg ->
                updateLocker msg model

            ValidatorError error ->
                childError "Validator" error

            ValidatorLog logInfo ->
                childLog "Validator" logInfo

            ValidatorMsg msg ->
                updateValidator msg model

            RetryConnectCmd retryCount failureMsg cmd ->
                let
                    parentMsg =
                        case failureMsg of
                            PGConnectError commandId ( _, error ) ->
                                logMsg commandId ("initCommand Error:" +-+ "Connection Error:" +-+ error +-+ "Connection Retry:" +-+ retryCount)

                            _ ->
                                Debug.crash "BUG -- Should never get here"
                in
                    ( model ! [ cmd ], [ parentMsg ] )

            RetryMsg commandId msg ->
                updateRetry commandId msg model



-- API


{-|
    initCommand
-}
initCommand : Config msg -> DbConnectionInfo -> Model -> ( Model, Cmd msg, Int )
initCommand config dbConnectionInfo model =
    let
        commandId =
            model.nextCommandId

        ( retryModel, retryCmd ) =
            Retry.retry (retryConfig config commandId) Retry.initModel (PGConnectError commandId) RetryConnectCmd (connectCmd dbConnectionInfo commandId)
    in
        ( { model | retryModels = Dict.insert commandId retryModel model.retryModels, nextCommandId = model.nextCommandId + 1 }
        , Cmd.map config.routeToMeTagger <| retryCmd
        , commandId
        )


{-|
    lockEntities
-}
lockAndValidateEntities : Config msg -> Model -> CommandId -> List EntityLockAndValidation -> Result String ( Model, Cmd msg )
lockAndValidateEntities config model commandId entityLockAndValidations =
    let
        lock commandState =
            let
                lockEntityIds =
                    getLockEntityIds entityLockAndValidations

                -- Sort entities to fail earlier and remove dups
                ( lockerModel, cmd ) =
                    Locker.lock (lockerConfig config) model.lockerModel commandId commandState.connectionId ((Set.toList << Set.fromList) lockEntityIds)
            in
                ( { model | lockerModel = lockerModel, commandStates = Dict.insert commandId { commandState | entityLockAndValidations = entityLockAndValidations } model.commandStates }, Cmd.map config.routeToMeTagger cmd )
    in
        Dict.get commandId model.commandStates
            |?> (\commandState -> Ok <| lock commandState)
            ?= badCommandId commandId


{-|
    writeEvents
-}
writeEvents : Config msg -> Model -> CommandId -> List String -> Result String ( Model, Cmd msg )
writeEvents config model commandId events =
    let
        writeEventsCmd commandId connectionId events =
            let
                statement =
                    insertEventsStatement events
            in
                Cmd.map config.routeToMeTagger <| query commandId (BeginError commandId statement) (Begin commandId statement) connectionId "BEGIN" 1
    in
        Dict.get commandId model.commandStates
            |?> (\commandState -> Ok ( model, (writeEventsCmd commandId commandState.connectionId events) ))
            ?= badCommandId commandId


{-|
    commit
-}
commit : Config msg -> Model -> CommandId -> Result String ( Model, Cmd msg )
commit config model commandId =
    Dict.get commandId model.commandStates
        |?> (\commandState -> Ok ( model, (Cmd.map config.routeToMeTagger <| query commandId (CommitError commandId) (Commit commandId) commandState.connectionId "COMMIT" 1) ))
        ?= badCommandId commandId


{-|
    rollback
-}
rollback : Config msg -> Model -> CommandId -> Result String ( Model, Cmd msg )
rollback config model commandId =
    Dict.get commandId model.commandStates
        |?> (\commandState -> Ok ( model, (Cmd.map config.routeToMeTagger <| query commandId (RollbackError commandId) (Rollback commandId) commandState.connectionId "ROLLBACK" 1) ))
        ?= badCommandId commandId



-- PRIVATE API


getLockEntityIds : List EntityLockAndValidation -> List EntityId
getLockEntityIds entityLockAndValidations =
    let
        crash event error =
            Debug.crash ("Illegal event:" +-+ event +-+ "Error:" +-+ error)

        getEntityIds : EntityLockAndValidation -> EntityId
        getEntityIds entityLockAndValidation =
            case entityLockAndValidation of
                LockAndValidate validationEvent ->
                    case validationEvent of
                        ValidateForMutation event ->
                            getEntityId event ??= crash event

                        ValidateExistence entityId ->
                            entityId

                LockOnly entityId ->
                    entityId

        crashIfInvalid entityId =
            (entityId == "")
                ?! ( \_ ->
                        let
                            crash =
                                Debug.crash "Entity Validations must not have blank Entity Ids"
                        in
                            False
                   , (\_ -> True)
                   )
    in
        entityLockAndValidations
            |> List.map getEntityIds
            |> List.filter crashIfInvalid


badCommandId : CommandId -> Result String x
badCommandId commandId =
    Err <| "CommandId:" +-+ commandId +-+ "doesn't exist"


insertEventsStatement : List String -> String
insertEventsStatement events =
    let
        createEvents event newEvents =
            let
                i =
                    List.length events - List.length newEvents
            in
                "($1[" +++ i +++ "]," +++ "$2,'" +++ event +++ "')" :: newEvents

        newEventList =
            events
                |> List.foldr createEvents []
                |> String.join ","
    in
        "SELECT insert_events($$" +++ newEventList +++ "$$)"


connectCmd : DbConnectionInfo -> CommandId -> FailureTagger ( ConnectionId, String ) Msg -> Cmd Msg
connectCmd dbConnectionInfo commandId failureTagger =
    Postgres.connect failureTagger
        (PGConnect commandId)
        (PGConnectionLost commandId)
        dbConnectionInfo.timeout
        dbConnectionInfo.host
        dbConnectionInfo.port_
        dbConnectionInfo.database
        dbConnectionInfo.user
        dbConnectionInfo.password


query : CommandId -> Postgres.ErrorTagger msg -> QueryTagger msg -> ConnectionId -> Sql -> Int -> Cmd msg
query commandId errorTagger queryTagger connectionId sql =
    Postgres.query errorTagger queryTagger connectionId ("-- Validator:: (CommandId, ConnectionId):" +-+ ( commandId, connectionId ) ++ "\n" ++ sql)
