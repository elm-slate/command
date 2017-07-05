port module Test.App exposing (..)

import Platform
import Time exposing (Time, second)
import Process
import Task exposing (Task)
import Dict exposing (Dict)
import ParentChildUpdate exposing (..)
import Slate.Common.Db exposing (..)
import Slate.Common.Event exposing (..)
import Slate.Command.Common.Command exposing (..)
import Slate.Command.Helper as CommandHelper exposing (EntityLockAndValidation(..))
import Slate.Command.Processor as CommandProcessor exposing (..)
import Slate.Command.Common.Validation as Validation exposing (..)
import Utils.Error exposing (..)
import Utils.Log exposing (..)
import StringUtils exposing ((+-+), (+++))
import DebugF exposing (..)


port exitApp : Float -> Cmd msg


port externalStop : (() -> msg) -> Sub msg


dbConnectionInfo : DbConnectionInfo
dbConnectionInfo =
    { host = "postgresDBServer"
    , port_ = 5432
    , database = "test_entities"
    , user = "charles"
    , password = "testpassword"
    , timeout = 5000
    }


commandProcessorConfig : CommandProcessor.Config String Msg
commandProcessorConfig =
    { connectionRetryMax = 3
    , routeToMeTagger = CommandProcessorMsg
    , errorTagger = CommandProcessorError
    , logTagger = CommandProcessorLog
    , commandErrorTagger = CommandExecutionError
    , commandSuccessTagger = CommandExecutionSuccess
    , schemaDict = Dict.empty
    , queryBatchSize = Nothing
    , debug = True
    }


type alias Model =
    { commandProcessorModel : CommandProcessor.Model String Msg
    }


type Msg
    = Nop
    | DoCmd (Cmd Msg)
    | StartApp
    | CommandProcessorError ( ErrorType, ( CommandId, String ) )
    | CommandProcessorLog ( LogLevel, ( CommandId, String ) )
    | CommandProcessorMsg (CommandProcessor.Msg String)
    | CommandExecutionError ( CommandId, CommandError String )
    | CommandExecutionSuccess CommandId
    | DummyValidate (CustomValidationErrorTagger String (CommandProcessor.Msg String)) (CustomValidationSuccessTagger (CommandProcessor.Msg String)) CommandId DbConnectionInfo


initModel : ( Model, List (Cmd Msg) )
initModel =
    let
        ( commandProcessorModel, commandProcessorCmd ) =
            CommandProcessor.init commandProcessorConfig
    in
        ( { commandProcessorModel = commandProcessorModel
          }
        , [ commandProcessorCmd ]
        )


init : ( Model, Cmd Msg )
init =
    let
        ( model, cmds ) =
            initModel
    in
        model ! (List.append cmds [ delayUpdateMsg StartApp (1 * second) ])


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


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    let
        updateCommandProcessor =
            ParentChildUpdate.updateChildApp (CommandProcessor.update commandProcessorConfig) update .commandProcessorModel CommandProcessorMsg (\model commandProcessorModel -> { model | commandProcessorModel = commandProcessorModel })
    in
        case msg of
            Nop ->
                model ! []

            DoCmd cmd ->
                model ! [ cmd ]

            StartApp ->
                let
                    -- mutatingEventInfo =
                    --     ( "999888777", "Create person", CreateEntity "Person" "123" )
                    -- mutatingEventInfo =
                    --     ( "999888777", "Add person name", AddProperty "Person" "123" "name" "Joe Mama" )
                    -- mutatingEventInfo =
                    --     ( "999888777", "Remove person name", RemoveProperty "Person" "123" "name" )
                    mutatingEventInfo =
                        ( "999888777", "Destroy person", DestroyEntity "Person" "123" )

                    mutating ( initiator, command, mutatingEvent ) =
                        Mutating mutatingEvent (Metadata initiator command)

                    lockAndValidateEvent mutatingEventInfo =
                        LockAndValidate << ValidateForMutation <| mutating mutatingEventInfo

                    entityLockAndValidations =
                        [ lockAndValidateEvent mutatingEventInfo
                        ]

                    events =
                        [ encodeEvent <| mutating mutatingEventInfo
                        ]

                    ( commandProcessorModel, cmd, commandId ) =
                        CommandProcessor.process commandProcessorConfig dbConnectionInfo (Just DummyValidate) entityLockAndValidations events model.commandProcessorModel
                in
                    { model | commandProcessorModel = commandProcessorModel } ! [ cmd ]

            CommandProcessorError ( errorType, details ) ->
                let
                    l =
                        case errorType of
                            NonFatalError ->
                                DebugF.log "CommandProcessorError" details

                            _ ->
                                Debug.crash <| toString details
                in
                    model ! []

            CommandProcessorLog ( logLevel, details ) ->
                let
                    l =
                        DebugF.log "CommandProcessorLog" (toString logLevel ++ ":" +-+ details)
                in
                    model ! []

            CommandExecutionError ( commandId, commandError ) ->
                let
                    l =
                        Debug.log "CommandExecutionError" ( commandId, commandError )
                in
                    model ! []

            CommandExecutionSuccess commandId ->
                let
                    l =
                        Debug.log "CommandSuccess" commandId
                in
                    model ! []

            CommandProcessorMsg msg ->
                updateCommandProcessor msg model

            DummyValidate errorTagger successTagger commandId dbConnectionInfo ->
                let
                    l =
                        Debug.log "DummyValidate" ""
                in
                    -- updateCommandProcessor (errorTagger (commandId, "bad things would happen")) model
                    updateCommandProcessor (successTagger commandId) model


subscriptions : Model -> Sub Msg
subscriptions model =
    Sub.none
