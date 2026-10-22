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
    , okfBundles =
      [ Schema.OkfBundle::{
        , name = "adrs"
        , path = "docs/adr"
        , profileBinding = Some
            ( Schema.ProfileBinding.Published
                Schema.PinnedImport::{
                , publisher = "shinzui/okf-profiles"
                , publisherRef = Some Schema.MoriRef::{
                  , namespace = "shinzui"
                  , name = "okf-profiles"
                  }
                , export = Some "documentation.architectureDecisions"
                , version = Some "v0.19.0"
                , pin = Some
                    "sha256:85176d78369b6d73c9f13c30277903b629d6bf048a4c7d71fc26e68b99c3eaa6"
                }
            )
        , okfVersion = "0.2"
        , description = Some "Durable architecture decisions"
        }
      , Schema.OkfBundle::{
        , name = "improvement-requests"
        , path = "docs/improvement-requests"
        , profileBinding = Some
            ( Schema.ProfileBinding.Published
                Schema.PinnedImport::{
                , publisher = "shinzui/okf-profiles"
                , publisherRef = Some Schema.MoriRef::{
                  , namespace = "shinzui"
                  , name = "okf-profiles"
                  }
                , export = Some "coordination.improvementRequests"
                , version = Some "v0.18.0"
                , pin = Some
                    "sha256:7d3a4a22be12fd0e697d6012ed1eb2efe4cb5dc4700d08fd49aa5e4c0e523df8"
                }
            )
        , okfVersion = "0.2"
        , description = Some
            "Requested changes to rdkafka-hs, carried over from the hw-kafka-client fork, with stable IR-N handles"
        }
      ]
    }
