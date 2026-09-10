use anyhow::{bail, Context, Result};
use serde::Serialize;
use std::{
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

use super::{
    contract::{EntityFileDescriptorV1, StateExportManifestV1},
    hashing::{sha256_bytes, sha256_file},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    Zstd,
    None,
}

impl Compression {
    pub fn extension(self) -> &'static str {
        match self {
            Self::Zstd => ".zst",
            Self::None => "",
        }
    }

    pub fn manifest_name(self) -> Option<String> {
        match self {
            Self::Zstd => Some("zstd".to_owned()),
            Self::None => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChunkLimits {
    pub max_records: u64,
    pub max_uncompressed_bytes: u64,
}

impl Default for ChunkLimits {
    fn default() -> Self {
        Self {
            max_records: 50_000,
            max_uncompressed_bytes: 128 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PartSummary {
    pub entity_type: String,
    pub part_number: u32,
    pub relative_filename: String,
    pub first_source_key: Option<String>,
    pub last_source_key: Option<String>,
    pub record_count: u64,
    pub uncompressed_bytes: u64,
    pub compressed_bytes: u64,
    pub sha256_checksum: String,
}

impl From<&PartSummary> for EntityFileDescriptorV1 {
    fn from(part: &PartSummary) -> Self {
        Self {
            filename: part.relative_filename.clone(),
            entity_type: part.entity_type.clone(),
            compression_type: if part.relative_filename.ends_with(".zst") {
                Some("zstd".to_owned())
            } else {
                None
            },
            record_count: part.record_count,
            sha256_checksum: part.sha256_checksum.clone(),
            min_source_key: part.first_source_key.clone(),
            max_source_key: part.last_source_key.clone(),
        }
    }
}

pub struct EntityPartWriter {
    base_dir: PathBuf,
    entity_type: String,
    compression: Compression,
    compression_level: i32,
    limits: ChunkLimits,
    part_number: u32,
    current_records: Vec<Vec<u8>>,
    current_bytes: u64,
    current_first_key: Option<String>,
    current_last_key: Option<String>,
    parts: Vec<PartSummary>,
}

impl EntityPartWriter {
    pub fn new(
        base_dir: PathBuf,
        entity_type: impl Into<String>,
        compression: Compression,
        compression_level: i32,
        limits: ChunkLimits,
    ) -> Self {
        Self {
            base_dir,
            entity_type: entity_type.into(),
            compression,
            compression_level,
            limits,
            part_number: 0,
            current_records: Vec::new(),
            current_bytes: 0,
            current_first_key: None,
            current_last_key: None,
            parts: Vec::new(),
        }
    }

    pub fn push<T: Serialize>(&mut self, source_key: &str, record: &T) -> Result<()> {
        let mut line = serde_json::to_vec(record)?;
        line.push(b'\n');
        let line_len = line.len() as u64;
        if !self.current_records.is_empty()
            && (self.current_records.len() as u64 >= self.limits.max_records
                || self.current_bytes + line_len > self.limits.max_uncompressed_bytes)
        {
            self.flush_part()?;
        }
        if self.current_first_key.is_none() {
            self.current_first_key = Some(source_key.to_owned());
        }
        self.current_last_key = Some(source_key.to_owned());
        self.current_bytes += line_len;
        self.current_records.push(line);
        Ok(())
    }

    pub fn finish(mut self) -> Result<Vec<PartSummary>> {
        if !self.current_records.is_empty() || self.parts.is_empty() {
            self.flush_part()?;
        }
        Ok(self.parts)
    }

    fn flush_part(&mut self) -> Result<()> {
        self.part_number += 1;
        let entity_dir = self.base_dir.join(&self.entity_type);
        fs::create_dir_all(&entity_dir)?;
        let filename = format!(
            "{}/part-{:06}.ndjson{}",
            self.entity_type,
            self.part_number,
            self.compression.extension()
        );
        let final_path = self.base_dir.join(&filename);
        let temp_path = final_path.with_extension("tmp");

        let mut raw = Vec::with_capacity(self.current_bytes as usize);
        for line in &self.current_records {
            raw.extend_from_slice(line);
        }

        let compressed = match self.compression {
            Compression::Zstd => zstd::stream::encode_all(&raw[..], self.compression_level)?,
            Compression::None => raw.clone(),
        };

        {
            let file = File::create(&temp_path)
                .with_context(|| format!("creating {}", temp_path.display()))?;
            let mut writer = BufWriter::new(file);
            writer.write_all(&compressed)?;
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }
        fs::rename(&temp_path, &final_path).with_context(|| {
            format!(
                "renaming completed part {} to {}",
                temp_path.display(),
                final_path.display()
            )
        })?;

        let checksum = sha256_file(&final_path)?;
        let summary = PartSummary {
            entity_type: self.entity_type.clone(),
            part_number: self.part_number,
            relative_filename: filename,
            first_source_key: self.current_first_key.take(),
            last_source_key: self.current_last_key.take(),
            record_count: self.current_records.len() as u64,
            uncompressed_bytes: self.current_bytes,
            compressed_bytes: compressed.len() as u64,
            sha256_checksum: checksum,
        };
        self.parts.push(summary);
        self.current_records.clear();
        self.current_bytes = 0;
        Ok(())
    }
}

pub fn write_manifest(package_dir: &Path, manifest: &StateExportManifestV1) -> Result<String> {
    let bytes = serde_json::to_vec_pretty(manifest)?;
    let checksum = sha256_bytes(&bytes);
    let manifest_tmp = package_dir.join("manifest.json.tmp");
    let manifest_path = package_dir.join("manifest.json");
    fs::write(&manifest_tmp, &bytes)?;
    File::open(&manifest_tmp)?.sync_all()?;
    fs::rename(&manifest_tmp, &manifest_path)?;

    let checksum_tmp = package_dir.join("manifest.sha256.tmp");
    let checksum_path = package_dir.join("manifest.sha256");
    fs::write(&checksum_tmp, format!("{checksum}  manifest.json\n"))?;
    File::open(&checksum_tmp)?.sync_all()?;
    fs::rename(&checksum_tmp, &checksum_path)?;
    Ok(checksum)
}

pub fn verify_package(path: &Path) -> Result<StateExportManifestV1> {
    let manifest_path = path.join("manifest.json");
    let checksum_path = path.join("manifest.sha256");
    if !manifest_path.exists() || !checksum_path.exists() {
        bail!("package is incomplete: manifest.json and manifest.sha256 are required");
    }
    let manifest_bytes = fs::read(&manifest_path)?;
    let expected_manifest = fs::read_to_string(checksum_path)?
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_owned();
    let actual_manifest = sha256_bytes(&manifest_bytes);
    if actual_manifest != expected_manifest {
        bail!("manifest checksum mismatch");
    }
    let manifest: StateExportManifestV1 = serde_json::from_slice(&manifest_bytes)?;
    manifest.validate()?;
    for file in &manifest.files {
        let part_path = path.join(&file.filename);
        let actual = sha256_file(&part_path)?;
        if actual != file.sha256_checksum {
            bail!("part checksum mismatch for {}", file.filename);
        }
    }
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn chunk_limits_are_enforced_without_splitting_records() {
        let temp = tempfile::tempdir().unwrap();
        let mut writer = EntityPartWriter::new(
            temp.path().to_path_buf(),
            "counties",
            Compression::None,
            0,
            ChunkLimits {
                max_records: 2,
                max_uncompressed_bytes: 1_000_000,
            },
        );
        writer.push("a", &json!({"source_key": "a"})).unwrap();
        writer.push("b", &json!({"source_key": "b"})).unwrap();
        writer.push("c", &json!({"source_key": "c"})).unwrap();
        let parts = writer.finish().unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].record_count, 2);
        assert_eq!(parts[1].record_count, 1);
        let first = fs::read_to_string(temp.path().join(&parts[0].relative_filename)).unwrap();
        assert!(first
            .lines()
            .all(|line| serde_json::from_str::<serde_json::Value>(line).is_ok()));
    }

    #[test]
    fn byte_limits_are_enforced() {
        let temp = tempfile::tempdir().unwrap();
        let mut writer = EntityPartWriter::new(
            temp.path().to_path_buf(),
            "documents",
            Compression::None,
            0,
            ChunkLimits {
                max_records: 100,
                max_uncompressed_bytes: 40,
            },
        );
        writer
            .push("a", &json!({"source_key": "a", "text": "long"}))
            .unwrap();
        writer
            .push("b", &json!({"source_key": "b", "text": "long"}))
            .unwrap();
        let parts = writer.finish().unwrap();
        assert_eq!(parts.len(), 2);
    }
}
