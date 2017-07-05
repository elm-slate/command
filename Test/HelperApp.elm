port module Test.HelperApp exposing (..)

import Platform
import Time exposing (Time, second)
import Process
import Task exposing (Task)
import Dict
import StringUtils exposing ((+-+), (+++))
import Slate.Command.Helper as CommandHelper exposing (EntityLockAndValidation(..))
import Slate.Command.Common.Command exposing (..)
import Slate.Command.Common.Validation as Validation exposing (..)
import ParentChildUpdate exposing (..)
import Utils.Ops exposing (..)
import Utils.Error exposing (..)
import Utils.Log exposing (..)
import Slate.Common.Db exposing (..)
import Slate.Common.Event exposing (..)
import DebugF exposing (..)


port exitApp : Float -> Cmd msg


port externalStop : (() -> msg) -> Sub msg


entityId1 : String
entityId1 =
    "7859eea1-d1ee-46be-865d-b04e0a12df4c"


entityId2 : String
entityId2 =
    "31cd9070-9073-415e-889f-dc0278dc7283"


dbConnectionInfo : DbConnectionInfo
dbConnectionInfo =
    { host = "postgresDBServer"
    , port_ = 5432
    , database = "test_entities"
    , user = "charles"
    , password = "testpassword"
    , timeout = 5000
    }


commandHelperConfig : CommandHelper.Config Msg
commandHelperConfig =
    { retryMax = Nothing
    , delayNext = Nothing
    , lockRetries = Nothing
    , routeToMeTagger = CommandHelperMsg
    , errorTagger = CommandHelperError
    , logTagger = CommandHelperLog
    , initCommandTagger = InitCommand
    , initCommandErrorTagger = InitCommandError
    , lockAndValidateSuccessTagger = LockAndValidateSuccess
    , lockErrorTagger = LockEntitiesError
    , validateErrorTagger = ValidateEntitiesError
    , writeEventsTagger = WriteEvents
    , writeEventsErrorTagger = WriteEventsError
    , commitTagger = Commit
    , commitErrorTagger = CommitError
    , rollbackTagger = Rollback
    , rollbackErrorTagger = RollbackError
    , connectionLostTagger = ConnectionLost
    , schemaDict = Dict.empty
    , queryBatchSize = Nothing
    , debug = True
    }


type alias Model =
    { commandHelperModel : CommandHelper.Model
    }


initModel : ( Model, List (Cmd Msg) )
initModel =
    let
        ( commandHelperModel, commandHelperCmd ) =
            CommandHelper.init commandHelperConfig
    in
        ( { commandHelperModel = commandHelperModel
          }
        , [ commandHelperCmd ]
        )


init : ( Model, Cmd Msg )
init =
    let
        ( model, cmds ) =
            initModel
    in
        model ! (List.append cmds [ delayUpdateMsg InitCommandStart (1 * second) ])


delayUpdateMsg : Msg -> Time -> Cmd Msg
delayUpdateMsg msg delay =
    Task.perform (\_ -> msg) <| Process.sleep delay


delayCmd : Cmd Msg -> Time -> Cmd Msg
delayCmd cmd =
    delayUpdateMsg <| DoCmd cmd


main : Program Never Model Msg
main =
    Platform.program
        { init = init
        , update = update
        , subscriptions = subscriptions
        }


type Msg
    = Nop
    | DoCmd (Cmd Msg)
    | CommandHelperError ( ErrorType, ( CommandId, String ) )
    | CommandHelperLog ( LogLevel, ( CommandId, String ) )
    | InitCommandStart
    | InitCommand CommandId
    | InitCommandError ( CommandId, String )
    | LockAndValidateSuccess CommandId
    | LockEntitiesError ( CommandId, String )
    | ValidateEntitiesError ( CommandId, List ( Event, String ) )
    | WriteEvents ( CommandId, Int )
    | WriteEventsError ( CommandId, String )
    | Commit CommandId
    | CommitError ( CommandId, String )
    | Rollback CommandId
    | RollbackError ( CommandId, String )
    | ConnectionLost ( CommandId, String )
    | CommandHelperMsg CommandHelper.Msg


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    let
        makeEvent mutatingEvent =
            Mutating mutatingEvent (Metadata "initiator" "command")

        updateCommandHelper =
            ParentChildUpdate.updateChildApp (CommandHelper.update commandHelperConfig) update .commandHelperModel CommandHelperMsg (\model commandHelperModel -> { model | commandHelperModel = commandHelperModel })
    in
        case msg of
            Nop ->
                model ! []

            DoCmd cmd ->
                model ! [ cmd ]

            CommandHelperError ( errorType, details ) ->
                let
                    l =
                        case errorType of
                            NonFatalError ->
                                DebugF.log "CommandHelperError" details

                            _ ->
                                Debug.crash <| toString details
                in
                    model ! []

            CommandHelperLog ( logLevel, details ) ->
                let
                    l =
                        DebugF.log "CommandHelperLog" (toString logLevel ++ ":" +-+ details)
                in
                    model ! []

            InitCommandStart ->
                let
                    l =
                        Debug.log "InitCommandStart" "Calling CommandHelper.initCommand"

                    ( commandHelperModel, cmd, commandId ) =
                        CommandHelper.initCommand commandHelperConfig dbConnectionInfo model.commandHelperModel
                in
                    { model | commandHelperModel = commandHelperModel } ! [ cmd ]

            InitCommand commandId ->
                let
                    l =
                        Debug.log "InitCommand Complete" ("Command Id:  " +-+ commandId)

                    ( commandHelperModel, cmd ) =
                        let
                            lockAndValidateEvent mutatingEvent =
                                LockAndValidate << ValidateForMutation <| Mutating mutatingEvent (Metadata "initiator" "command")
                        in
                            CommandHelper.lockAndValidateEntities commandHelperConfig model.commandHelperModel commandId [ lockAndValidateEvent <| CreateEntity "entityName" entityId1, lockAndValidateEvent <| CreateEntity "entityName" entityId2 ]
                                ??= (\err ->
                                        Debug.crash ("CommandHelper.lockEntities call returned Error:" +-+ err +-+ "CommandId:" +-+ commandId)
                                    )
                in
                    { model | commandHelperModel = commandHelperModel } ! [ cmd ]

            InitCommandError ( commandId, error ) ->
                let
                    l =
                        Debug.log "InitCommand Complete with Error" ("Command Id:  " +-+ commandId +-+ "Error:" +-+ error)
                in
                    ( model, delayCmd (exitApp 1) (1 * second) )

            LockAndValidateSuccess commandId ->
                let
                    l =
                        Debug.log "lockAndValidateEntities Complete" ("Command Id:  " +-+ commandId)

                    events =
                        [ encodeEvent <| makeEvent <| CreateEntity "entityName" entityId1
                        , encodeEvent <| makeEvent <| CreateEntity "entityName" entityId2
                          -- encodeEvent "User Created" entityId1 "Create User" "64194fcb-bf87-40c2-bee7-3a86f0110840"
                          -- , encodeEvent "User Created" entityId2 "Create User" "d2a1cf24-dc3a-45d6-8310-1fb6eb184d1b"
                        ]

                    ( commandHelperModel, cmd ) =
                        CommandHelper.writeEvents commandHelperConfig model.commandHelperModel commandId events
                            ??= (\err ->
                                    Debug.crash ("CommandHelper.writeEvents call returned Error:" +-+ err +-+ "CommandId:" +-+ commandId)
                                )
                in
                    { model | commandHelperModel = commandHelperModel } ! [ cmd ]

            LockEntitiesError ( commandId, error ) ->
                let
                    l =
                        Debug.log "lockAndValidateEntities Complete with Error" ("Command Id:  " +-+ commandId +-+ "Error:" +-+ error)

                    ( commandHelperModel, cmd ) =
                        CommandHelper.rollback commandHelperConfig model.commandHelperModel commandId
                            ??= (\err ->
                                    Debug.crash ("CommandHelper.rollback call returned Error:" +-+ err +-+ "CommandId:" +-+ commandId)
                                )
                in
                    { model | commandHelperModel = commandHelperModel } ! [ cmd ]

            ValidateEntitiesError ( commandId, error ) ->
                let
                    l =
                        Debug.log "lockAndValidateEntities Complete with Error" ("Command Id:  " +-+ commandId +-+ "Error:" +-+ error)

                    ( commandHelperModel, cmd ) =
                        CommandHelper.rollback commandHelperConfig model.commandHelperModel commandId
                            ??= (\err ->
                                    Debug.crash ("CommandHelper.rollback call returned Error:" +-+ err +-+ "CommandId:" +-+ commandId)
                                )
                in
                    { model | commandHelperModel = commandHelperModel } ! [ cmd ]

            WriteEvents ( commandId, eventRows ) ->
                let
                    l =
                        Debug.log "WriteEvents Complete" ("Command Id:" +-+ commandId +-+ "Events Inserted:" +-+ eventRows)

                    ( commandHelperModel, cmd ) =
                        CommandHelper.commit commandHelperConfig model.commandHelperModel commandId
                            ??= (\err ->
                                    Debug.crash ("CommandHelper.commit call returned Error:" +-+ err +-+ "CommandId:" +-+ commandId)
                                )
                in
                    { model | commandHelperModel = commandHelperModel } ! [ cmd ]

            WriteEventsError ( commandId, error ) ->
                let
                    l =
                        Debug.log "WriteEvents Complete with Error" ("Command Id:" +-+ commandId +-+ "Error:" +-+ error)

                    ( commandHelperModel, cmd ) =
                        CommandHelper.rollback commandHelperConfig model.commandHelperModel commandId
                            ??= (\err ->
                                    Debug.crash ("CommandHelper.rollback call returned Error:" +-+ err +-+ "CommandId:" +-+ commandId)
                                )
                in
                    { model | commandHelperModel = commandHelperModel } ! [ cmd ]

            Commit commandId ->
                let
                    l =
                        Debug.log "Commit Complete" ("Command Id:  " +-+ commandId)
                in
                    ( model, delayCmd (exitApp 0) (1 * second) )

            CommitError ( commandId, error ) ->
                let
                    l =
                        Debug.log "Commit Complete with Error" ("Command Id:" +-+ commandId +-+ "Error:" +-+ error)

                    ( commandHelperModel, cmd ) =
                        CommandHelper.rollback commandHelperConfig model.commandHelperModel commandId
                            ??= (\err ->
                                    Debug.crash ("CommandHelper.rollback call returned Error:" +-+ err +-+ "CommandId:" +-+ commandId)
                                )
                in
                    { model | commandHelperModel = commandHelperModel } ! [ cmd ]

            Rollback commandId ->
                ( model, delayCmd (exitApp 1) (1 * second) )

            RollbackError ( commandId, error ) ->
                let
                    l =
                        Debug.log "Rollback Complete with Error" ("Command Id:  " +-+ commandId +-+ "Error:" +-+ error)
                in
                    ( model, delayCmd (exitApp 1) (1 * second) )

            ConnectionLost ( commandId, error ) ->
                let
                    l =
                        Debug.log "ConnectionLost" ("Command Id:  " +-+ commandId +-+ "Error:" +-+ error)
                in
                    ( model, delayCmd (exitApp 1) (1 * second) )

            CommandHelperMsg msg ->
                updateCommandHelper msg model


subscriptions : Model -> Sub Msg
subscriptions model =
    Sub.none
