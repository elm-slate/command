module Slate.Command.Validator
    exposing
        ( Msg
        , Model
        , Config
        , init
        , update
        , validate
        , entityNoLongerExistsError
        , alreadyExistsError
        , doesNotExistError
        , outOfBoundsError
        )

import Set
import Tuple exposing (..)
import Dict exposing (Dict)
import Json.Decode as JD exposing (field)
import StringUtils exposing (..)
import Utils.Error exposing (..)
import Utils.Ops exposing (..)
import Utils.Json exposing (..)
import Utils.Log exposing (..)
import Utils.Regex as RegexU
import Slate.Common.Taggers exposing (..)
import Slate.Common.Entity exposing (..)
import Slate.Common.Event exposing (..)
import Slate.Command.Common.Command exposing (..)
import Slate.Command.Common.Validation exposing (..)
import Postgres exposing (..)


-- import DebugF


batchSize : Int
batchSize =
    1000


type alias ValidationQueryResult =
    { type_ : String
    , entityId : String
    , target : String
    , operation : String
    , propertyName : Maybe String
    , propertyId : Maybe String
    }


type alias ValidationState =
    { entityValidationEvents : List ValidationEvent
    , results : List String
    }


type alias ValidationDict =
    Dict ConnectionId ValidationState


type SlateObject
    = Entity EntityId
    | Property EntityId PropertyName
    | PropertyListItem EntityId PropertyName PropertyId
    | Relationship EntityId PropertyName
    | RelationshipListItem EntityId PropertyName PropertyId


{-|
parent msg taggers
-}
type alias RouteToMeTagger msg =
    Msg -> msg


{-|
    Validator Config.
-}
type alias Config msg =
    { routeToMeTagger : RouteToMeTagger msg
    , errorTagger : ErrorTagger ( CommandId, String ) msg
    , logTagger : LogTagger ( CommandId, String ) msg
    , validateEntitiesSuccessTagger : EntityValidationSuccessTagger msg
    , validateEntitiesErrorTagger : EntityValidationErrorTagger msg
    }


type Msg
    = Nop
    | QueryError CommandId ( ConnectionId, String )
    | QuerySuccess CommandId ( ConnectionId, List String )


type alias Model =
    { validations : ValidationDict
    }


init : (Msg -> msg) -> ( Model, Cmd msg )
init tagger =
    ({ validations = Dict.empty } ! [])


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

            QuerySuccess commandId ( connectionId, results ) ->
                let
                    validationState =
                        Dict.get commandId model.validations ?!= (\_ -> Debug.crash "Unable to get Validation State")

                    currentResults =
                        List.append results <| validationState.results

                    doValidation =
                        let
                            decodeCrash resultStr error =
                                let
                                    crash =
                                        Debug.crash ("Validation query decoding error:" +-+ error ++ "\non:" +-+ resultStr)
                                in
                                    ValidationQueryResult "" "" "" "" Nothing Nothing

                            queryResultSet =
                                List.map (\resultStr -> JD.decodeString validationQueryDecoder resultStr ??= decodeCrash currentResults) currentResults

                            doesSlateObjectExist existsInDb nowExists noLongerExists slateObject =
                                (existsInDb || List.member slateObject nowExists) && (not <| List.member slateObject noLongerExists)

                            entityExists entityId result =
                                result.type_ == "existence" && result.operation == "created" && result.entityId == entityId && result.target == "entity"

                            crash error =
                                Debug.crash "Program bug:" +-+ error

                            doesEntityExist nowExists noLongerExists event =
                                let
                                    entityId =
                                        getEntityId event ??= crash

                                    slateObject =
                                        Entity entityId

                                    existsInDb =
                                        List.any (entityExists entityId) queryResultSet
                                in
                                    ( doesSlateObjectExist existsInDb nowExists noLongerExists slateObject, slateObject )

                            entityUseToExist entityId result =
                                result.type_ == "existence" && result.operation == "destroyed" && result.entityId == entityId && result.target == "entity"

                            didEntityUseToExist nowExists noLongerExists event =
                                let
                                    entityId =
                                        getEntityId event ??= crash

                                    slateObject =
                                        Entity entityId

                                    existsInDb =
                                        List.any (entityUseToExist entityId) queryResultSet
                                in
                                    ( doesSlateObjectExist existsInDb nowExists noLongerExists slateObject, slateObject )

                            propertyExists entityId propertyName result =
                                result.type_ == "existence" && result.operation == "added" && result.entityId == entityId && result.target == "property" && result.propertyName == Just propertyName && result.propertyId == Nothing

                            doesPropertyExist nowExists noLongerExists event =
                                let
                                    entityId =
                                        getEntityId event ??= crash

                                    propertyName =
                                        getPropertyName event ??= crash

                                    slateObject =
                                        Property entityId propertyName

                                    existsInDb =
                                        List.any (propertyExists entityId propertyName) queryResultSet
                                in
                                    ( doesSlateObjectExist existsInDb nowExists noLongerExists slateObject, slateObject )

                            propertyItemExists entityId propertyName propertyId result =
                                result.type_ == "existence" && result.operation == "added" && result.entityId == entityId && result.target == "propertyList" && result.propertyName == Just propertyName && result.propertyId == Just propertyId

                            doesPropertyItemExist nowExists noLongerExists event =
                                let
                                    entityId =
                                        getEntityId event ??= crash

                                    propertyName =
                                        getPropertyName event ??= crash

                                    propertyId =
                                        getPropertyId event ??= crash

                                    slateObject =
                                        PropertyListItem entityId propertyName propertyId

                                    existsInDb =
                                        List.any (propertyItemExists entityId propertyName propertyId) queryResultSet
                                in
                                    ( doesSlateObjectExist existsInDb nowExists noLongerExists slateObject, slateObject )

                            countItemsInDb itemAdded entityId propertyName =
                                let
                                    items =
                                        List.foldl
                                            (\result dict ->
                                                result.propertyId
                                                    |?> (\propertyId -> itemAdded result entityId propertyName ? ( Dict.insert propertyId () dict, Dict.remove propertyId dict ))
                                                    ?= dict
                                            )
                                            Dict.empty
                                            queryResultSet
                                in
                                    List.length <| Dict.toList items

                            countItemsInTrans nowExists noLongerExists slateObject =
                                let
                                    extractSameListParts slateObject =
                                        case slateObject of
                                            PropertyListItem entityId propertyName _ ->
                                                ( entityId, propertyName )

                                            RelationshipListItem entityId propertyName _ ->
                                                ( entityId, propertyName )

                                            _ ->
                                                ( "", "" )

                                    nowExistsCount =
                                        nowExists
                                            |> List.foldl (\sObj count -> count + ((extractSameListParts sObj) == (extractSameListParts slateObject)) ? ( 1, 0 )) 0

                                    noLongerExistsCount =
                                        noLongerExists
                                            |> List.foldl (\sObj count -> count + ((extractSameListParts sObj) == (extractSameListParts slateObject)) ? ( 1, 0 )) 0
                                in
                                    nowExistsCount - noLongerExistsCount

                            propertyItemAddedForPositioning result entityId propertyName =
                                result.type_ == "positioning" && result.operation == "added" && result.entityId == entityId && result.target == "propertyList" && result.propertyName == Just propertyName && result.propertyId /= Nothing

                            isPropertyItemInBounds nowExists noLongerExists event =
                                let
                                    entityId =
                                        getEntityId event ??= crash

                                    propertyName =
                                        getPropertyName event ??= crash

                                    countInDb =
                                        countItemsInDb propertyItemAddedForPositioning entityId propertyName

                                    countInTrans =
                                        countItemsInTrans nowExists noLongerExists <| PropertyListItem entityId propertyName ""

                                    count =
                                        countInDb + countInTrans

                                    ( oldPosition, newPosition ) =
                                        getPosition event ??= (\error -> Debug.crash <| "Program bug:" +-+ error)
                                in
                                    oldPosition < count && newPosition < count && oldPosition >= 0 && newPosition >= 0

                            relationshipExists entityId propertyName result =
                                result.type_ == "existence" && result.operation == "added" && result.entityId == entityId && result.target == "relationship" && result.propertyName == Just propertyName && result.propertyId == Nothing

                            doesRelationshipExist nowExists noLongerExists event =
                                let
                                    entityId =
                                        getEntityId event ??= crash

                                    propertyName =
                                        getPropertyName event ??= crash

                                    slateObject =
                                        Relationship entityId propertyName

                                    existsInDb =
                                        List.any (relationshipExists entityId propertyName) queryResultSet
                                in
                                    ( doesSlateObjectExist existsInDb nowExists noLongerExists slateObject, slateObject )

                            relationshipItemExists entityId propertyName propertyId result =
                                result.type_ == "existence" && result.operation == "added" && result.entityId == entityId && result.target == "relationshipList" && result.propertyName == Just propertyName && result.propertyId == Just propertyId

                            doesRelationshipItemExist nowExists noLongerExists event =
                                let
                                    entityId =
                                        getEntityId event ??= crash

                                    propertyName =
                                        getPropertyName event ??= crash

                                    propertyId =
                                        getPropertyId event ??= crash

                                    slateObject =
                                        RelationshipListItem entityId propertyName propertyId

                                    existsInDb =
                                        List.any (relationshipItemExists entityId propertyName propertyId) queryResultSet
                                in
                                    ( doesSlateObjectExist existsInDb nowExists noLongerExists slateObject, slateObject )

                            relationshipItemAddedForPositioning result entityId propertyName =
                                result.type_ == "positioning" && result.operation == "added" && result.entityId == entityId && result.target == "relationshipList" && result.propertyName == Just propertyName && result.propertyId /= Nothing

                            isRelationshipItemInBounds nowExists noLongerExists event =
                                let
                                    entityId =
                                        getEntityId event ??= crash

                                    propertyName =
                                        getPropertyName event ??= crash

                                    countInDb =
                                        countItemsInDb relationshipItemAddedForPositioning entityId propertyName

                                    countInTrans =
                                        countItemsInTrans nowExists noLongerExists <| RelationshipListItem entityId propertyName ""

                                    count =
                                        countInDb + countInTrans

                                    ( oldPosition, newPosition ) =
                                        getPosition event ??= (\error -> Debug.crash <| "Program bug:" +-+ error)
                                in
                                    oldPosition < count && newPosition < count && oldPosition >= 0 && newPosition >= 0

                            validateEvent validationEvent ( nowExists, noLongerExists, errors ) =
                                case validationEvent of
                                    ValidateForMutation event ->
                                        case event of
                                            Mutating mutatingEvent _ ->
                                                let
                                                    entityExists entityId =
                                                        let
                                                            ( entityExists, _ ) =
                                                                doesEntityExist nowExists noLongerExists event
                                                        in
                                                            entityExists
                                                in
                                                    case mutatingEvent of
                                                        CreateEntity _ entityId ->
                                                            let
                                                                ( exists, slateObject ) =
                                                                    doesEntityExist nowExists noLongerExists event

                                                                ( existed, _ ) =
                                                                    didEntityUseToExist nowExists noLongerExists event
                                                            in
                                                                exists
                                                                    ? ( ( nowExists, noLongerExists, ( event, alreadyExistsError "entity" ) :: errors )
                                                                      , existed
                                                                            ? ( ( nowExists, noLongerExists, ( event, entityNoLongerExistsError ) :: errors )
                                                                              , ( slateObject :: nowExists, noLongerExists, errors )
                                                                              )
                                                                      )

                                                        DestroyEntity _ entityId ->
                                                            let
                                                                ( exists, slateObject ) =
                                                                    doesEntityExist nowExists noLongerExists event
                                                            in
                                                                exists
                                                                    ? ( ( nowExists, slateObject :: noLongerExists, errors )
                                                                      , ( nowExists, noLongerExists, ( event, doesNotExistError "entity" ) :: errors )
                                                                      )

                                                        AddProperty _ entityId propertyName _ ->
                                                            entityExists entityId
                                                                ? ( ( Property entityId propertyName :: nowExists, noLongerExists, errors )
                                                                  , ( nowExists, noLongerExists, ( event, doesNotExistError "entity" ) :: errors )
                                                                  )

                                                        RemoveProperty _ entityId propertyName ->
                                                            let
                                                                ( exists, slateObject ) =
                                                                    doesPropertyExist nowExists noLongerExists event
                                                            in
                                                                entityExists entityId
                                                                    ? ( exists
                                                                            ? ( ( nowExists, slateObject :: noLongerExists, errors )
                                                                              , ( nowExists, noLongerExists, ( event, doesNotExistError "property" ) :: errors )
                                                                              )
                                                                      , ( nowExists, noLongerExists, ( event, doesNotExistError "entity" ) :: errors )
                                                                      )

                                                        AddPropertyList _ entityId propertyName propertyId _ ->
                                                            let
                                                                ( exists, slateObject ) =
                                                                    doesPropertyItemExist nowExists noLongerExists event
                                                            in
                                                                entityExists entityId
                                                                    ? ( exists
                                                                            ? ( ( nowExists, noLongerExists, ( event, alreadyExistsError "property item" ) :: errors )
                                                                              , ( slateObject :: nowExists, noLongerExists, errors )
                                                                              )
                                                                      , ( nowExists, noLongerExists, ( event, doesNotExistError "entity" ) :: errors )
                                                                      )

                                                        RemovePropertyList _ entityId propertyName propertyId ->
                                                            let
                                                                ( exists, slateObject ) =
                                                                    doesPropertyItemExist nowExists noLongerExists event
                                                            in
                                                                entityExists entityId
                                                                    ? ( exists
                                                                            ? ( ( nowExists, slateObject :: noLongerExists, errors )
                                                                              , ( nowExists, noLongerExists, ( event, doesNotExistError "property item" ) :: errors )
                                                                              )
                                                                      , ( nowExists, noLongerExists, ( event, doesNotExistError "entity" ) :: errors )
                                                                      )

                                                        PositionPropertyList _ entityId _ _ ->
                                                            entityExists entityId
                                                                ? ( isPropertyItemInBounds nowExists noLongerExists event
                                                                        ? ( ( nowExists, noLongerExists, errors )
                                                                          , ( nowExists, noLongerExists, ( event, outOfBoundsError "property" ) :: errors )
                                                                          )
                                                                  , ( nowExists, noLongerExists, ( event, doesNotExistError "entity" ) :: errors )
                                                                  )

                                                        AddRelationship _ entityId propertyName _ ->
                                                            let
                                                                ( exists, slateObject ) =
                                                                    doesRelationshipExist nowExists noLongerExists event
                                                            in
                                                                entityExists entityId
                                                                    ? ( exists
                                                                            ? ( ( nowExists, noLongerExists, ( event, alreadyExistsError "relationship" ) :: errors )
                                                                              , ( slateObject :: nowExists, noLongerExists, errors )
                                                                              )
                                                                      , ( nowExists, noLongerExists, ( event, doesNotExistError "entity" ) :: errors )
                                                                      )

                                                        RemoveRelationship _ entityId _ ->
                                                            let
                                                                ( exists, slateObject ) =
                                                                    doesRelationshipExist nowExists noLongerExists event
                                                            in
                                                                entityExists entityId
                                                                    ? ( exists
                                                                            ? ( ( nowExists, slateObject :: noLongerExists, errors )
                                                                              , ( nowExists, noLongerExists, ( event, doesNotExistError "relationship" ) :: errors )
                                                                              )
                                                                      , ( nowExists, noLongerExists, ( event, doesNotExistError "entity" ) :: errors )
                                                                      )

                                                        AddRelationshipList _ entityId _ propertyId _ ->
                                                            let
                                                                ( exists, slateObject ) =
                                                                    doesRelationshipItemExist nowExists noLongerExists event
                                                            in
                                                                entityExists entityId
                                                                    ? ( exists
                                                                            ? ( ( nowExists, noLongerExists, ( event, alreadyExistsError "relationship item" ) :: errors )
                                                                              , ( slateObject :: nowExists, noLongerExists, errors )
                                                                              )
                                                                      , ( nowExists, noLongerExists, ( event, doesNotExistError "entity" ) :: errors )
                                                                      )

                                                        RemoveRelationshipList _ entityId _ propertyId ->
                                                            let
                                                                ( exists, slateObject ) =
                                                                    doesRelationshipItemExist nowExists noLongerExists event
                                                            in
                                                                entityExists entityId
                                                                    ? ( exists
                                                                            ? ( ( nowExists, slateObject :: noLongerExists, errors )
                                                                              , ( nowExists, noLongerExists, ( event, doesNotExistError "relationship item" ) :: errors )
                                                                              )
                                                                      , ( nowExists, noLongerExists, ( event, doesNotExistError "entity" ) :: errors )
                                                                      )

                                                        PositionRelationshipList _ entityId _ _ ->
                                                            entityExists entityId
                                                                ? ( isRelationshipItemInBounds nowExists noLongerExists event
                                                                        ? ( ( nowExists, noLongerExists, errors )
                                                                          , ( nowExists, noLongerExists, ( event, outOfBoundsError "relationship" ) :: errors )
                                                                          )
                                                                  , ( nowExists, noLongerExists, ( event, doesNotExistError "entity" ) :: errors )
                                                                  )

                                            NonMutating _ _ ->
                                                ( nowExists, noLongerExists, errors )

                                    ValidateExistence entityId ->
                                        let
                                            event =
                                                Mutating (CreateEntity "" entityId) <| Metadata "" ""

                                            ( exists, slateObject ) =
                                                doesEntityExist nowExists noLongerExists event
                                        in
                                            exists
                                                ? ( ( nowExists, noLongerExists, errors )
                                                  , ( nowExists, noLongerExists, ( event, doesNotExistError "entity" ) :: errors )
                                                  )

                            ( _, _, validationErrors ) =
                                validationState.entityValidationEvents
                                    |> List.foldl validateEvent ( [], [], [] )

                            validationResultMsg =
                                (validationErrors == []) ? ( config.validateEntitiesSuccessTagger commandId, config.validateEntitiesErrorTagger ( commandId, List.reverse validationErrors ) )
                        in
                            ( ( model, Cmd.none ), [ validationResultMsg ] )

                    getMore =
                        ( ( { model | validations = Dict.insert commandId { validationState | results = currentResults } model.validations }
                          , Postgres.moreQueryResults (QueryError commandId) (QuerySuccess commandId) connectionId
                          )
                        , []
                        )
                in
                    (List.length results /= batchSize)
                        ? ( doValidation
                          , getMore
                          )

            QueryError commandId ( connectionId, error ) ->
                ( model ! [], [ nonFatal commandId ("Query Error during validation:" +-+ error) ] )



-- API


entityNoLongerExistsError : String
entityNoLongerExistsError =
    "entity no longer exists"


alreadyExistsError : String -> String
alreadyExistsError object =
    object +-+ "already exists"


doesNotExistError : String -> String
doesNotExistError object =
    object +-+ "does not exist"


outOfBoundsError : String -> String
outOfBoundsError object =
    object +-+ "out of bounds"


validate : Config msg -> Model -> CommandId -> ConnectionId -> List ValidationEvent -> ( Model, Cmd msg )
validate config model commandId connectionId entityValidationEvents =
    let
        entityTemplate =
            """
                            OR (event#>>'{entityId}' = '{{entityId}}'
                                AND event#>>'{target}' = 'entity'
                                AND event#>>'{operation}' IN ('created', 'destroyed')
                            )
            """

        propertyTemplate =
            """
                            OR (event#>>'{entityId}' = '{{entityId}}'
                                AND event#>>'{target}' = 'property'
                                AND event#>>'{operation}' IN ('added', 'removed')
                                AND event#>>'{propertyName}' = '{{propertyName}}'
                            )
            """

        propertyItemTemplate =
            """
                            OR (event#>>'{entityId}' = '{{entityId}}'
                                AND event#>>'{target}' = 'propertyList'
                                AND event#>>'{operation}' IN ('added', 'removed')
                                AND event#>>'{propertyName}' = '{{propertyName}}'
                                AND event#>>'{propertyId}' = '{{propertyId}}'
                            )
            """

        propertyPositioningTemplate =
            """
                            OR (
                                event#>>'{entityId}' = '{{entityId}}'
                                AND event#>>'{target}' = 'propertyList'
                                AND event#>>'{propertyName}' = '{{propertyName}}'
                            )
            """

        relationshipTemplate =
            """
                            OR (event#>>'{entityId}' = '{{entityId}}'
                                AND event#>>'{target}' = 'relationship'
                                AND event#>>'{operation}' IN ('added', 'removed')
                                AND event#>>'{propertyName}' = '{{propertyName}}'
                            )
            """

        relationshipItemTemplate =
            """
                            OR (event#>>'{entityId}' = '{{entityId}}'
                                AND event#>>'{target}' = 'relationshipList'
                                AND event#>>'{operation}' IN ('added', 'removed')
                                AND event#>>'{propertyName}' = '{{propertyName}}'
                                AND event#>>'{propertyId}' = '{{propertyId}}'
                            )
            """

        relationshipPositioningTemplate =
            """
                            OR (
                                event#>>'{entityId}' = '{{entityId}}'
                                AND event#>>'{target}' = 'relationshipList'
                                AND event#>>'{propertyName}' = '{{propertyName}}'
                            )
            """

        sqlTemplate =
            """
                SELECT 'existence' AS "type", "entityId", "target", "propertyName", "operation", "propertyId"
                FROM (
                    SELECT "entityId", "target", "propertyName", "operation", "propertyId", RANK() OVER (PARTITION BY "entityId", "target", "propertyName", "propertyId" ORDER BY id DESC) as _rank
                    FROM (
                        SELECT  id,
                                event#>>'{entityId}' AS "entityId",
                                event#>>'{target}' AS "target",
                                event#>>'{operation}' AS "operation",
                                event#>>'{propertyName}' AS "propertyName",
                                event#>>'{propertyId}' AS "propertyId"
                        FROM events
                        WHERE 1!=1
                            {{clauses}}
                    ) AS qq
                ) AS q
                WHERE q._rank = 1
            """

        positioningSqlTemplate =
            """
                SELECT 'positioning' AS "type", "entityId", "target", "propertyName", "operation", "propertyId"
                FROM (
                    SELECT "entityId", "target", "propertyName", "operation", "propertyId", _rank
                    FROM (
                        SELECT "entityId", "target", "propertyName", "operation", "propertyId", RANK() OVER (PARTITION BY "entityId", "target", "propertyName", "propertyId" ORDER BY id DESC) as _rank
                        FROM (
                            SELECT  id,
                                event#>>'{entityId}' AS "entityId",
                                event#>>'{target}' AS "target",
                                event#>>'{operation}' AS "operation",
                                event#>>'{propertyName}' AS "propertyName",
                                event#>>'{propertyId}' AS "propertyId"
                            FROM events
                            WHERE 1!=1
                                {{positioningClauses}}
                        ) AS qqq
                    ) AS qq
                    WHERE operation = 'added'
                ) AS q
                WHERE q._rank = 1
            """

        buildClause validationEvent =
            let
                equalTo value =
                    ("=" +-+ "'" ++ value ++ "'")

                buildEntityTemplate entityId =
                    ( "clauses"
                    , RegexU.replaceAll "{{entityId}}" entityId entityTemplate
                    )

                buildPropertyTemplate entityId propertyName =
                    ( "clauses"
                    , RegexU.replaceAll "{{propertyName}}" propertyName <|
                        RegexU.replaceAll "{{entityId}}" entityId propertyTemplate
                    )

                buildPropertyItemTemplate entityId propertyName propertyId =
                    ( "clauses"
                    , RegexU.replaceAll "{{propertyId}}" propertyId <|
                        RegexU.replaceAll "{{propertyName}}" propertyName <|
                            RegexU.replaceAll "{{entityId}}" entityId propertyItemTemplate
                    )

                buildPropertyItemTemplateForPositioning entityId propertyName =
                    ( "positioningClauses"
                    , RegexU.replaceAll "{{propertyName}}" propertyName <|
                        RegexU.replaceAll "{{entityId}}" entityId propertyPositioningTemplate
                    )

                buildRelationshipTemplate entityId propertyName =
                    ( "clauses"
                    , RegexU.replaceAll "{{propertyName}}" propertyName <|
                        RegexU.replaceAll "{{entityId}}" entityId relationshipTemplate
                    )

                buildRelationshipItemTemplate entityId propertyName propertyId =
                    ( "clauses"
                    , RegexU.replaceAll "{{propertyId}}" propertyId <|
                        RegexU.replaceAll "{{propertyName}}" propertyName <|
                            RegexU.replaceAll "{{entityId}}" entityId relationshipItemTemplate
                    )

                buildRelationshipItemTemplateForPositioning entityId propertyName =
                    ( "positioningClauses"
                    , RegexU.replaceAll "{{propertyName}}" propertyName <|
                        RegexU.replaceAll "{{entityId}}" entityId relationshipPositioningTemplate
                    )
            in
                case validationEvent of
                    ValidateForMutation event ->
                        case event of
                            Mutating mutatingEvent _ ->
                                case mutatingEvent of
                                    CreateEntity _ entityId ->
                                        [ buildEntityTemplate entityId ]

                                    DestroyEntity _ entityId ->
                                        [ buildEntityTemplate entityId ]

                                    AddProperty _ entityId _ _ ->
                                        [ buildEntityTemplate entityId ]

                                    RemoveProperty _ entityId propertyName ->
                                        [ buildEntityTemplate entityId, buildPropertyTemplate entityId propertyName ]

                                    AddPropertyList _ entityId propertyName propertyId _ ->
                                        [ buildEntityTemplate entityId, buildPropertyItemTemplate entityId propertyName propertyId ]

                                    RemovePropertyList _ entityId propertyName propertyId ->
                                        [ buildEntityTemplate entityId, buildPropertyItemTemplate entityId propertyName propertyId ]

                                    PositionPropertyList _ entityId propertyName _ ->
                                        [ buildEntityTemplate entityId, buildPropertyItemTemplateForPositioning entityId propertyName ]

                                    AddRelationship _ entityId _ _ ->
                                        [ buildEntityTemplate entityId ]

                                    RemoveRelationship _ entityId propertyName ->
                                        [ buildEntityTemplate entityId, buildRelationshipTemplate entityId propertyName ]

                                    AddRelationshipList _ entityId propertyName propertyId _ ->
                                        [ buildEntityTemplate entityId, buildRelationshipItemTemplate entityId propertyName propertyId ]

                                    RemoveRelationshipList _ entityId propertyName propertyId ->
                                        [ buildEntityTemplate entityId, buildRelationshipItemTemplate entityId propertyName propertyId ]

                                    PositionRelationshipList _ entityId propertyName _ ->
                                        [ buildEntityTemplate entityId, buildRelationshipItemTemplateForPositioning entityId propertyName ]

                            NonMutating _ _ ->
                                [ ( "", "" ) ]

                    ValidateExistence entityId ->
                        [ buildEntityTemplate entityId ]

        entityClauses =
            entityValidationEvents
                |> List.map buildClause
                |> Set.fromList
                |> Set.toList

        getClauses type_ =
            entityClauses
                |> List.concat
                |> List.filter ((==) type_ << first)
                |> List.filter ((/=) "" << second)
                |> List.map second
                |> String.join "\n"

        clauses =
            getClauses "clauses"

        positioningClauses =
            getClauses "positioningClauses"

        unionAll =
            (clauses /= "" && positioningClauses /= "") ? ( "\n                UNION ALL\n", "" )

        sql =
            -- DebugF.log "sql" <|
            ((clauses == "") ? ( "", RegexU.replaceAll "{{clauses}}" clauses sqlTemplate ) ++ unionAll ++ (positioningClauses == "") ? ( "", RegexU.replaceAll "{{positioningClauses}}" positioningClauses positioningSqlTemplate ))
    in
        ( { model | validations = Dict.insert commandId (ValidationState entityValidationEvents []) model.validations }, Cmd.map config.routeToMeTagger <| Postgres.query (QueryError commandId) (QuerySuccess commandId) connectionId sql batchSize )



-- PRIVATE API


validationQueryDecoder : JD.Decoder ValidationQueryResult
validationQueryDecoder =
    JD.succeed ValidationQueryResult
        <|| (field "type" JD.string)
        <|| (field "entityId" JD.string)
        <|| (field "target" JD.string)
        <|| (field "operation" JD.string)
        <|| (JD.maybe <| field "propertyName" JD.string)
        <|| (JD.maybe <| field "propertyId" JD.string)
