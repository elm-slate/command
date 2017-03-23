module Slate.Command.Common.Validation
    exposing
        ( ValidateTagger
        , EntityValidationSuccessTagger
        , EntityValidationErrorTagger
        , CustomValidationSuccessTagger
        , CustomValidationErrorTagger
        , ValidationError
        , ValidationEvent(..)
        )

{-|
    Common Validator definitions.

@docs ValidateTagger, EntityValidationSuccessTagger , EntityValidationErrorTagger , CustomValidationSuccessTagger , CustomValidationErrorTagger, ValidationError, ValidationEvent
-}

import Slate.Command.Common.Command exposing (..)
import Slate.Common.Db exposing (..)
import Slate.Common.Event exposing (..)
import Slate.Common.Entity exposing (..)


{-|
    Tagger to create a message to signal that entity validation succeeded.
-}
type alias EntityValidationSuccessTagger commandProcessorMsg =
    CommandId -> commandProcessorMsg


{-|
    Tagger to create a message to signal that a validation failed.
-}
type alias EntityValidationErrorTagger msg =
    ( CommandId, List ( Event, String ) ) -> msg


{-|
    Tagger to create a message to signal that a custom validation succeeded.
-}
type alias CustomValidationSuccessTagger commandProcessorMsg =
    CommandId -> commandProcessorMsg


{-|
    Tagger to create a message to signal that a custom validation failed.
-}
type alias CustomValidationErrorTagger customValidationError msg =
    ( CommandId, customValidationError ) -> msg


{-|
    Tagger to create Command Processor's Parent Msg
-}
type alias ValidateTagger commandProcessorMsg customValidationError msg =
    CustomValidationErrorTagger customValidationError commandProcessorMsg
    -> CustomValidationSuccessTagger commandProcessorMsg
    -> CommandId
    -> DbConnectionInfo
    -> msg


{-|
    Validation Error.
-}
type ValidationError
    = ValidationError Event


{-|
    Validation event.
-}
type ValidationEvent
    = ValidateForMutation Event
    | ValidateExistence EntityId
