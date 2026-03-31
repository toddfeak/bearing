//! Ground-up rebuild of the indexing pipeline.

pub mod analyzer;
pub mod channel;
pub(crate) mod codecs;
pub mod config;
pub mod consumer;
pub mod coordinator;
pub mod default_worker_factory;
pub mod directory;
pub mod directory_adapter;
pub mod document;
pub mod field;
pub mod field_info_registry;
pub mod field_infos_consumer;
pub mod id_generator;
pub mod index_file_names;
pub mod norms_consumer;
pub mod per_field_postings;
pub mod postings_consumer;
pub mod segment;
pub mod segment_accumulator;
pub mod segment_context;
pub mod segment_infos;
pub mod segment_worker;
pub mod standard_analyzer;
pub mod stored_fields_consumer;
pub mod writer;
