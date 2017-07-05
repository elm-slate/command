module Slate.Command.Processor
    exposing
        ( Msg
        , Model
        , Config
        , CommandErrorTagger
        , CommandSuccessTagger
        , CommandError(..)
        , init
        , update
        , process
        )

{-|
    Command Processor for Entities.

@docs Msg , Model , Config, CommandErrorTagger, CommandSuccessTagger, CommandError, init, update, process
-}

import Dict as Dict exposing (Dict)
import StringUtils exposing ((+-+), (+++))
import Slate.Command.Helper as CommandHelper exposing (EntityLockAndValidation(..))
import Slate.Command.Common.Validation exposing (..)
import Slate.Command.Common.Command exposing (..)
import ParentChildUpdate exposing (..)
import Utils.Ops exposing (..)
import Utils.Error exposing (..)
import Utils.Log exposing (..)
import Slate.Common.Db exposing (..)
import Slate.Common.Event exposing (..)
import Slate.Common.Entity exposing (..)
import Slate.Common.Schema exposing (..)
import Slate.Common.Taggers exposing (..)
import DebugF exposing (..)


commandHelperConfig : Config customValidationError msg -> CommandHelper.Config (Msg customValidationError)
commandHelperConfig config =
    { retryMax = Just config.connectionRetryMax
    , delayNext = Nothing
    , lockRetries = Nothing
    , routeToMeTagger = CommandHelperMsg
    , errorTagger = CommandHelperError
    , logTagger = CommandHelperLog
    , initCommandTagger = InitCommandSuccess
    , initCommandErrorTagger = InitCommandError
    , lockAndValidateSuccessTagger = LockAndValidateSuccess
    , lockErrorTagger = LockEntitiesError
    , validateErrorTagger = ValidateEntitiesError
    , writeEventsTagger = WriteEventsSuccess
    , writeEventsErrorTagger = WriteEventsError
    , commitTagger = CommitSuccess
    , commitErrorTagger = CommitError
    , rollbackTagger = RollbackSuccess
    , rollbackErrorTagger = RollbackError
    , connectionLostTagger = ConnectionLost
    , schemaDict = config.schemaDict
    , queryBatchSize = config.queryBatchSize
    , debug = config.debug
    }


type alias CommandState customValidationError msg =
    { dbConnectionInfo : DbConnectionInfo
    , entityLockAndValidations : List EntityLockAndValidation
    , events : List String
    , maybeValidateTagger : Maybe (ValidateTagger (Msg customValidationError) customValidationError msg)
    , commandError : Maybe (CommandError customValidationError)
    }


type alias CommandStateDict customValidationError msg =
    Dict CommandId (CommandState customValidationError msg)


{-|
    Command Processing Error type.
-}
type CommandError customValidationError
    = OperationalCommandError String
    | EntityValidationCommandError (List ( Event, String ))
    | CustomValidationCommandError customValidationError


{-|
    Command Processor's Config
-}
type alias Config customValidationError msg =
    { connectionRetryMax : Int
    , routeToMeTagger : RouteToMeTagger customValidationError msg
    , errorTagger : ErrorTagger ( CommandId, String ) msg
    , logTagger : LogTagger ( CommandId, String ) msg
    , commandErrorTagger : CommandErrorTagger customValidationError msg
    , commandSuccessTagger : CommandSuccessTagger msg
    , schemaDict : Dict EntityName EntitySchema
    , queryBatchSize : Maybe Int
    , debug : Bool
    }


{-|
    Command Processor's Model
-}
type alias Model customValidationError msg =
    { commandHelperModel : CommandHelper.Model
    , commandStates : CommandStateDict customValidationError msg
    }


initModel : Config customValidationError msg -> ( Model customValidationError msg, List (Cmd (Msg customValidationError)) )
initModel config =
    let
        ( commandHelperModel, commandHelperCmds ) =
            CommandHelper.init (commandHelperConfig config)
    in
        ( { commandHelperModel = commandHelperModel
          , commandStates = Dict.empty
          }
        , [ commandHelperCmds ]
        )


{-|
    Command Processor's Msg
-}
type Msg customValidationError
    = Nop
    | InitCommandSuccess CommandId
    | InitCommandError ( CommandId, String )
    | LockAndValidateSuccess CommandId
    | LockEntitiesError ( CommandId, String )
    | ValidateEntitiesError ( CommandId, List ( Event, String ) )
    | WriteEventsSuccess ( CommandId, Int )
    | WriteEventsError ( CommandId, String )
    | CommitSuccess CommandId
    | CommitError ( CommandId, String )
    | RollbackSuccess CommandId
    | RollbackError ( CommandId, String )
    | ConnectionLost ( CommandId, String )
    | ValidationSuccess CommandId
    | ValidationError ( CommandId, customValidationError )
    | CommandHelperError ( ErrorType, ( CommandId, String ) )
    | CommandHelperLog ( LogLevel, ( CommandId, String ) )
    | CommandHelperMsg CommandHelper.Msg


{-|
    Update.
-}
update : Config customValidationError msg -> Msg customValidationError -> Model customValidationError msg -> ( ( Model customValidationError msg, Cmd (Msg customValidationError) ), List msg )
update config msg model =
    let
        crash error =
            Debug.crash error

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
                            crash "Unexpected error type"
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
                            crash "Unexpected log level"
            in
                ( model ! [], [ logHandler commandId (module_ ++ ":" +-+ ( commandId, message )) ] )

        getCommandState commandId =
            Dict.get commandId model.commandStates
                ?!= (\_ -> crash ("BUG -- Command Id:" +-+ commandId +-+ "not in dictionary" +-+ (toStringF model.commandStates)))

        getEntityLockAndValidations commandId =
            .entityLockAndValidations <| getCommandState commandId

        events commandId =
            .events <| getCommandState commandId

        commandFailedMsg commandId error =
            config.commandErrorTagger ( commandId, error )

        removeCommand model commandId =
            { model | commandStates = Dict.remove commandId model.commandStates }

        rollbackLessFailure model commandId maybeRollbackError originalError =
            let
                msgs =
                    maybeRollbackError
                        |?> (\rollbackError -> [ nonFatal commandId rollbackError ])
                        ?= []
            in
                ( removeCommand model commandId ! [ Cmd.none ], List.append msgs [ commandFailedMsg commandId originalError ] )

        helperFailed model commandId originalError =
            let
                ( ( commandHelperModel, cmd ), msgs ) =
                    CommandHelper.rollback (commandHelperConfig config) model.commandHelperModel commandId
                        |??> (\( commandHelperModel, cmd ) -> ( { model | commandHelperModel = commandHelperModel } ! [ cmd ], [] ))
                        ??= (\rollbackError -> rollbackLessFailure model commandId (Just rollbackError) originalError)

                commandState =
                    getCommandState commandId
            in
                ( { model | commandStates = Dict.insert commandId { commandState | commandError = Just originalError } model.commandStates } ! [ cmd ], msgs )

        helperResults model commandId result =
            let
                ( ( newModel, cmd ), msgs ) =
                    result
                        |??> (\( commandHelperModel, cmd ) -> ( { model | commandHelperModel = commandHelperModel } ! [ cmd ], [] ))
                        ??= (helperFailed model commandId << OperationalCommandError)
            in
                ( newModel ! [ cmd ], msgs )

        getCommandErrorMsg commandId =
            let
                commandState =
                    getCommandState commandId
            in
                commandState.commandError ?!= (\_ -> crash ("No Command Error for commandState:" +-+ commandState))

        updateCommandHelper =
            updateChildParent (CommandHelper.update (commandHelperConfig config)) (update config) .commandHelperModel CommandHelperMsg (\model commandHelperModel -> { model | commandHelperModel = commandHelperModel })
    in
        case msg of
            Nop ->
                ( model ! [], [] )

            InitCommandSuccess commandId ->
                let
                    commandState =
                        getCommandState commandId

                    crashInfo =
                        "(CommandId:" +-+ commandId +-+ ", CommandState:" +-+ commandState +-+ ")"

                    entityLockAndValidations =
                        getEntityLockAndValidations commandId

                    entityIdError =
                        "Entity Id cannot be an empty string"

                    mustNotBeBlank ( errorMsg, str ) =
                        (str == "") ? ( [ errorMsg ], [] )

                    entityIdCheck entityId =
                        mustNotBeBlank ( entityIdError, entityId )

                    validateEvent : Event -> List String
                    validateEvent event =
                        let
                            entityNameError =
                                "Entity Name cannot be an empty string"

                            propertyNameError =
                                "Property Name cannot be an empty string"

                            propertyIdError =
                                "Property Id cannot be an empty string"

                            relationshipIdError =
                                "Relationship Id cannot be an empty string"

                            positionCheck oldPosition newPosition =
                                let
                                    negativeError =
                                        (oldPosition <= 0 || newPosition <= 0) ? ( [], [ "Both oldPosition and newPosition must be positive" ] )
                                in
                                    [ negativeError ]

                            entityCheck entityName entityId =
                                List.concat <| List.map mustNotBeBlank [ ( entityNameError, entityName ), ( entityIdError, entityId ) ]

                            propertyCheck entityName entityId propertyName =
                                List.concat <| List.map mustNotBeBlank [ ( entityNameError, entityName ), ( entityIdError, entityId ), ( propertyNameError, propertyName ) ]

                            propertyListCheck entityName entityId propertyName propertyId =
                                List.concat <| List.map mustNotBeBlank [ ( entityNameError, entityName ), ( entityIdError, entityId ), ( propertyNameError, propertyName ), ( propertyIdError, propertyId ) ]

                            positionPropertyListCheck entityName entityId propertyName ( oldPosition, newPosition ) =
                                List.concat <| List.append (positionCheck oldPosition newPosition) <| List.map mustNotBeBlank [ ( entityNameError, entityName ), ( entityIdError, entityId ), ( propertyNameError, propertyName ) ]

                            relationshipIdCheck maybeRelationshipId =
                                maybeRelationshipId |?> (\relationshipId -> [ ( relationshipIdError, relationshipId ) ]) ?= []

                            relationshipCheck entityName entityId propertyName maybeRelationshipId =
                                List.concat <| List.map mustNotBeBlank <| List.append (relationshipIdCheck maybeRelationshipId) [ ( entityNameError, entityName ), ( entityIdError, entityId ), ( propertyNameError, propertyName ) ]

                            relationshipListCheck entityName entityId propertyName propertyId maybeRelationshipId =
                                List.concat <| List.map mustNotBeBlank <| List.append (relationshipIdCheck maybeRelationshipId) [ ( entityNameError, entityName ), ( entityIdError, entityId ), ( propertyNameError, propertyName ), ( propertyIdError, propertyId ) ]

                            positionRelationshipListCheck entityName entityId propertyName ( oldPosition, newPosition ) =
                                List.concat <| List.append (positionCheck oldPosition newPosition) <| List.map mustNotBeBlank [ ( entityNameError, entityName ), ( entityIdError, entityId ), ( propertyNameError, propertyName ) ]
                        in
                            case event of
                                Mutating mutatingEvent _ ->
                                    case mutatingEvent of
                                        CreateEntity entityName entityId ->
                                            entityCheck entityName entityId

                                        DestroyEntity entityName entityId ->
                                            entityCheck entityName entityId

                                        AddProperty entityName entityId propertyName _ ->
                                            propertyCheck entityName entityId propertyName

                                        RemoveProperty entityName entityId propertyName ->
                                            propertyCheck entityName entityId propertyName

                                        AddPropertyList entityName entityId propertyName propertyId _ ->
                                            propertyListCheck entityName entityId propertyName propertyId

                                        RemovePropertyList entityName entityId propertyName propertyId ->
                                            propertyListCheck entityName entityId propertyName propertyId

                                        PositionPropertyList entityName entityId propertyName position ->
                                            positionPropertyListCheck entityName entityId propertyName position

                                        AddRelationship entityName entityId propertyName relationshipId ->
                                            relationshipCheck entityName entityId propertyName (Just relationshipId)

                                        RemoveRelationship entityName entityId propertyName ->
                                            relationshipCheck entityName entityId propertyName Nothing

                                        AddRelationshipList entityName entityId propertyName propertyId relationshipId ->
                                            relationshipListCheck entityName entityId propertyName propertyId (Just relationshipId)

                                        RemoveRelationshipList entityName entityId propertyName propertyId ->
                                            relationshipListCheck entityName entityId propertyName propertyId Nothing

                                        PositionRelationshipList entityName entityId propertyName position ->
                                            positionRelationshipListCheck entityName entityId propertyName position

                                NonMutating _ _ ->
                                    []

                    validateEntityLockAndValidations lockAndValidation =
                        case lockAndValidation of
                            LockAndValidate validationEvent ->
                                case validationEvent of
                                    ValidateForMutation event ->
                                        validateEvent event

                                    ValidateExistence entityId ->
                                        entityIdCheck entityId

                            LockOnly entityId ->
                                entityIdCheck entityId

                    errors =
                        List.foldl (\lockAndValidation errors -> List.append errors <| validateEntityLockAndValidations lockAndValidation) [] entityLockAndValidations

                    errorCrash =
                        (errors /= []) ?! ( (\_ -> crash <| "Invalid Entity Operations\n    " ++ (String.join "\n    " errors) +-+ "\n    " ++ crashInfo ++ "\n"), identity )
                in
                    CommandHelper.lockAndValidateEntities (commandHelperConfig config) model.commandHelperModel commandId (getEntityLockAndValidations commandId)
                        |> helperResults model commandId

            InitCommandError ( commandId, error ) ->
                rollbackLessFailure model commandId Nothing <| OperationalCommandError error

            LockAndValidateSuccess commandId ->
                let
                    commandState =
                        getCommandState commandId
                in
                    commandState.maybeValidateTagger
                        |?> (\validateTagger ->
                                ( model ! []
                                , [ validateTagger ValidationError ValidationSuccess commandId commandState.dbConnectionInfo ]
                                )
                            )
                        ?= update config (ValidationSuccess commandId) model

            LockEntitiesError ( commandId, error ) ->
                helperFailed model commandId <| OperationalCommandError error

            ValidateEntitiesError ( commandId, errors ) ->
                helperFailed model commandId <| EntityValidationCommandError errors

            WriteEventsSuccess ( commandId, eventRows ) ->
                CommandHelper.commit (commandHelperConfig config) model.commandHelperModel commandId
                    |> helperResults model commandId

            WriteEventsError ( commandId, error ) ->
                helperFailed model commandId <| OperationalCommandError error

            CommitSuccess commandId ->
                ( removeCommand model commandId ! [], [ config.commandSuccessTagger commandId ] )

            CommitError ( commandId, error ) ->
                helperFailed model commandId <| OperationalCommandError error

            RollbackSuccess commandId ->
                ( removeCommand model commandId ! [], [ commandFailedMsg commandId <| getCommandErrorMsg commandId ] )

            RollbackError ( commandId, error ) ->
                ( removeCommand model commandId ! [], [ nonFatal commandId error, commandFailedMsg commandId <| getCommandErrorMsg commandId ] )

            ConnectionLost ( commandId, error ) ->
                rollbackLessFailure model commandId Nothing <| OperationalCommandError error

            ValidationSuccess commandId ->
                CommandHelper.writeEvents (commandHelperConfig config) model.commandHelperModel commandId (events commandId)
                    |> helperResults model commandId

            ValidationError ( commandId, error ) ->
                helperFailed model commandId <| CustomValidationCommandError error

            CommandHelperError error ->
                childError "CommandHelper" error

            CommandHelperLog logInfo ->
                childLog "CommandHelper" logInfo

            CommandHelperMsg msg ->
                updateCommandHelper msg model



-- API


{-|
    Parent's Tagger that will result in calling this modules update function.
-}
type alias RouteToMeTagger customValidationError msg =
    Msg customValidationError -> msg


{-|
    Tagger for command errors.
-}
type alias CommandErrorTagger customValidationError msg =
    ( CommandId, CommandError customValidationError ) -> msg


{-|
    Tagger for command success.
-}
type alias CommandSuccessTagger msg =
    CommandId -> msg


{-|
    Initialize command processor
-}
init : Config customValidationError msg -> ( Model customValidationError msg, Cmd msg )
init config =
    let
        ( model, cmds ) =
            initModel config
    in
        model ! (List.map (Cmd.map config.routeToMeTagger) cmds)


{-|
    Process command.
-}
process : Config customValidationError msg -> DbConnectionInfo -> Maybe (ValidateTagger (Msg customValidationError) customValidationError msg) -> List EntityLockAndValidation -> List String -> Model customValidationError msg -> ( Model customValidationError msg, Cmd msg, CommandId )
process config dbConnectionInfo maybeValidateTagger entityLockAndValidations events model =
    let
        ( commandHelperModel, cmd, commandId ) =
            CommandHelper.initCommand (commandHelperConfig config) dbConnectionInfo model.commandHelperModel

        commandStates =
            Dict.insert commandId
                { dbConnectionInfo = dbConnectionInfo
                , entityLockAndValidations = entityLockAndValidations
                , events = events
                , maybeValidateTagger = maybeValidateTagger
                , commandError = Nothing
                }
                model.commandStates
    in
        ( { model | commandHelperModel = commandHelperModel, commandStates = commandStates }, Cmd.map config.routeToMeTagger cmd, commandId )
