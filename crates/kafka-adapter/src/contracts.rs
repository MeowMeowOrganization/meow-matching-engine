//! Generated Protobuf DTOs for the Kafka boundary.
//!
//! These types remain transport DTOs. The deterministic engine receives only
//! `meow_matching_engine::Command` and returns its own domain result types.

pub mod meow {
    pub mod exchange {
        pub mod order {
            #[allow(clippy::doc_markdown, clippy::must_use_candidate)]
            pub mod v1alpha1 {
                include!(concat!(env!("OUT_DIR"), "/meow.exchange.order.v1alpha1.rs"));
            }
        }

        pub mod matching {
            #[allow(clippy::doc_markdown, clippy::must_use_candidate)]
            pub mod v1alpha1 {
                include!(concat!(
                    env!("OUT_DIR"),
                    "/meow.exchange.matching.v1alpha1.rs"
                ));
            }
        }
    }
}

pub use meow::exchange::matching::v1alpha1 as matching;
pub use meow::exchange::order::v1alpha1 as order;
