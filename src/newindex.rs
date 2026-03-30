//! Ground-up rebuild of the indexing pipeline.

pub mod analyzer;
pub mod channel;
pub(crate) mod codecs;
pub mod config;
pub mod consumer;
pub mod coordinator;
pub mod directory;
pub mod directory_adapter;
pub mod document;
pub mod field;
pub mod field_info_registry;
pub mod id_generator;
pub mod index_file_names;
pub mod random_id_generator;
pub mod segment;
pub mod segment_accumulator;
pub mod segment_infos;
pub mod segment_worker;
pub mod writer;
