let Schema =
      https://raw.githubusercontent.com/shinzui/mori-schema/3522f4a51181d73c9c90fc27a7c0838bd29ae95f/package.dhall
        sha256:dcb19e2312e790bad14e622cc98a1281cd2298c5b564a2f0d0534d3c718d8803

in  Schema.Project::{
    , project = Schema.ProjectIdentity::{
      , name = "rdkafka-hs"
      , namespace = "shinzui"
      , stableId = Some "project_01m5gb4fh5evrvxea4v135d8ab"
      , type = Schema.PackageType.Library
      , language = Schema.Language.Haskell
      , lifecycle = Schema.Lifecycle.Experimental
      , description = Some
          "Haskell bindings to librdkafka, forked from hw-kafka-client."
      , domains = [ "kafka", "messaging" ]
      , owners = [ "shinzui" ]
      , origin = Schema.Origin.Fork
      , upstream = Some Schema.MoriRef::{
        , namespace = "haskell-works"
        , name = "hw-kafka-client"
        }
      }
    , repos =
      [ Schema.Repo::{
        , name = "rdkafka-hs"
        , github = Some "shinzui/rdkafka-hs"
        , localPath = Some "."
        }
      ]
    , packages =
      [ Schema.Package::{
        , name = "rdkafka-hs"
        , type = Schema.PackageType.Library
        , language = Schema.Language.Haskell
        , path = Some "."
        , description = Some "Kafka bindings for Haskell backed by librdkafka"
        }
      ]
    , dependencies = [ "confluentinc/librdkafka" ]
    , dependencyRefs =
      [ Schema.MoriRef::{ namespace = "confluentinc", name = "librdkafka" } ]
    }
