module Kafka.Producer.ProducerPropertiesSpec (tests) where

import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Kafka.PropertySupport
import Test.Tasty (TestTree)
import Test.Tasty.HUnit (testCase, (@?=))
import Test.Tasty.QuickCheck qualified as QC
import "rdkafka-hs" Kafka.Producer qualified as P

data Edit = Global Text Text | Topic Text Text | Level P.KafkaLogLevel
  deriving stock (Eq, Show)

tests :: TestTree
tests =
  generatedGroup
    "Producer properties generated"
    [ QC.testProperty "program follows last-write semantics" $
        QC.forAllShrink program shrinkProgram $ \edits ->
          view (assemble edits) QC.=== model edits,
      QC.testProperty "configuration has left and right identity" $
        QC.forAllShrink program shrinkProgram $ \edits ->
          let props = assemble edits
           in QC.conjoin [view (mempty <> props) QC.=== view props, view (props <> mempty) QC.=== view props],
      QC.testProperty "configuration composition is associative" $
        QC.forAllShrink triple shrinkTriple $ \(a, b, c) ->
          view ((assemble a <> assemble b) <> assemble c) QC.=== view (assemble a <> (assemble b <> assemble c)),
      QC.testProperty "composition appends writes in order" $
        QC.forAllShrink pair shrinkPair $ \(a, b) ->
          view (assemble a <> assemble b) QC.=== model (a <> b),
      QC.testProperty "unrelated keys survive a conflicting write" $
        QC.forAllShrink ((,) <$> smallText <*> smallText) shrinkValues $ \(before, after) ->
          let left =
                P.extraProps (Map.fromList [("left", before), ("shared", before)])
                  <> P.extraTopicProps (Map.fromList [("left", before), ("shared", before)])
              right = P.extraProp "shared" after <> P.extraTopicProps (Map.singleton "shared" after)
              expected = Map.fromList [("left", before), ("shared", after)]
           in view (left <> right) QC.=== (expected, expected, Nothing),
      QC.testProperty "last explicit log level wins" $
        QC.forAll ((,) <$> logLevel <*> logLevel) $ \(a, b) ->
          P.ppLogLevel (P.logLevel a <> P.logLevel b) QC.=== Just b,
      testCase "fixed empty duplicate and binary text values" $ do
        let edits = [Global "" "", Global "shared" "before", Global "shared" "after", Topic "empty" "", Topic "binary" "a\0b", Level P.KafkaLogDebug, Level P.KafkaLogEmerg]
        view (assemble edits) @?= (Map.fromList [("", ""), ("shared", "after")], Map.fromList [("empty", ""), ("binary", "a\0b")], Just P.KafkaLogEmerg)
    ]

program :: QC.Gen [Edit]
program = boundedList 30 $ QC.oneof [Global <$> smallText <*> smallText, Topic <$> smallText <*> smallText, Level <$> logLevel]

logLevel :: QC.Gen P.KafkaLogLevel
logLevel = QC.elements [P.KafkaLogEmerg, P.KafkaLogAlert, P.KafkaLogCrit, P.KafkaLogErr, P.KafkaLogWarning, P.KafkaLogNotice, P.KafkaLogInfo, P.KafkaLogDebug]

shrinkProgram :: [Edit] -> [[Edit]]
shrinkProgram = QC.shrinkList shrinkEdit
  where
    shrinkEdit (Global key value) = [Global key' value | key' <- shrinkText key] <> [Global key value' | value' <- shrinkText value]
    shrinkEdit (Topic key value) = [Topic key' value | key' <- shrinkText key] <> [Topic key value' | value' <- shrinkText value]
    shrinkEdit (Level _) = []

pair :: QC.Gen ([Edit], [Edit])
pair = (,) <$> program <*> program

triple :: QC.Gen ([Edit], [Edit], [Edit])
triple = (,,) <$> program <*> program <*> program

shrinkPair :: ([Edit], [Edit]) -> [([Edit], [Edit])]
shrinkPair (a, b) = [(a', b) | a' <- shrinkProgram a] <> [(a, b') | b' <- shrinkProgram b]

shrinkTriple :: ([Edit], [Edit], [Edit]) -> [([Edit], [Edit], [Edit])]
shrinkTriple (a, b, c) = [(a', b, c) | a' <- shrinkProgram a] <> [(a, b', c) | b' <- shrinkProgram b] <> [(a, b, c') | c' <- shrinkProgram c]

shrinkValues :: (Text, Text) -> [(Text, Text)]
shrinkValues (a, b) = [(a', b) | a' <- shrinkText a] <> [(a, b') | b' <- shrinkText b]

assemble :: [Edit] -> P.ProducerProperties
assemble = foldMap $ \case
  Global key value -> P.extraProp key value
  Topic key value -> P.extraTopicProps (Map.singleton key value)
  Level level -> P.logLevel level

view :: P.ProducerProperties -> (Map Text Text, Map Text Text, Maybe P.KafkaLogLevel)
view props = (P.ppKafkaProps props, P.ppTopicProps props, P.ppLogLevel props)

-- Independent state-machine model: each edit writes one field/key. It does
-- not use producer composition or map union to derive expected results.
model :: [Edit] -> (Map Text Text, Map Text Text, Maybe P.KafkaLogLevel)
model = foldl' step (Map.empty, Map.empty, Nothing)
  where
    step (global, topic, level) = \case
      Global key value -> (Map.insert key value global, topic, level)
      Topic key value -> (global, Map.insert key value topic, level)
      Level newLevel -> (global, topic, Just newLevel)
