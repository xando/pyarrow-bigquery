// Re-export the generated proto modules with friendlier paths.

#![allow(clippy::all)]
#![allow(rustdoc::all)]

pub mod google {
    pub mod api {
        tonic::include_proto!("google.api");
    }
    pub mod rpc {
        tonic::include_proto!("google.rpc");
    }
    pub mod cloud {
        pub mod bigquery {
            pub mod storage {
                pub mod v1 {
                    tonic::include_proto!("google.cloud.bigquery.storage.v1");
                }
            }
        }
    }
}

pub use google::cloud::bigquery::storage::v1 as bqstorage_v1;
