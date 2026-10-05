//! File category classification and automatic folder routing based on extensions and MIME types.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Categorization of downloaded files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, Hash)]
pub enum FileCategory {
    /// General or uncategorized downloads.
    #[default]
    General,
    /// Documents and text files (PDF, DOCX, TXT, etc.).
    Documents,
    /// Archives and disk images (ZIP, TAR, ISO, etc.).
    Compressed,
    /// Video media (MP4, MKV, WEBM, etc.).
    Video,
    /// Audio media (MP3, FLAC, WAV, etc.).
    Audio,
    /// Static and vector images (PNG, JPG, SVG, etc.).
    Images,
    /// Executables, installers, and packages (EXE, DEB, RPM, etc.).
    Programs,
}

impl FileCategory {
    /// Detects file category by filename extension with optional MIME type fallback.
    pub fn from_filename_or_mime(filename: &str, mime: Option<&str>) -> Self {
        if let Some((_, ext)) = filename.rsplit_once('.') {
            let ext_lower = ext.trim().to_ascii_lowercase();
            match ext_lower.as_str() {
                // Documents
                "pdf" | "doc" | "docx" | "xls" | "xlsx" | "ppt" | "pptx" | "txt" | "epub"
                | "csv" | "odt" | "rtf" | "md" | "pages" | "numbers" | "key" => {
                    return Self::Documents;
                }
                // Compressed
                "zip" | "rar" | "7z" | "tar" | "gz" | "bz2" | "xz" | "iso" | "tgz" | "tbz2"
                | "zst" | "cab" => {
                    return Self::Compressed;
                }
                // Video
                "mp4" | "mkv" | "avi" | "mov" | "wmv" | "flv" | "webm" | "m4v" | "3gp" | "ts"
                | "vob" => {
                    return Self::Video;
                }
                // Audio
                "mp3" | "wav" | "flac" | "aac" | "ogg" | "m4a" | "wma" | "opus" | "alac"
                | "aiff" => {
                    return Self::Audio;
                }
                // Images
                "jpg" | "jpeg" | "png" | "gif" | "webp" | "svg" | "bmp" | "ico" | "tiff"
                | "tif" | "avif" | "heic" => {
                    return Self::Images;
                }
                // Programs / Installers
                "exe" | "msi" | "apk" | "deb" | "rpm" | "appimage" | "sh" | "bin" | "dmg"
                | "pkg" | "cmd" | "bat" => {
                    return Self::Programs;
                }
                _ => {}
            }
        }

        if let Some(m) = mime {
            let m_lower = m.trim().to_ascii_lowercase();
            if m_lower.starts_with("video/") {
                return Self::Video;
            }
            if m_lower.starts_with("audio/") {
                return Self::Audio;
            }
            if m_lower.starts_with("image/") {
                return Self::Images;
            }
            if m_lower.starts_with("text/")
                || m_lower.contains("pdf")
                || m_lower.contains("document")
            {
                return Self::Documents;
            }
            if m_lower.contains("zip")
                || m_lower.contains("tar")
                || m_lower.contains("compressed")
                || m_lower.contains("archive")
            {
                return Self::Compressed;
            }
        }

        Self::General
    }

    /// Default relative subfolder name for this category.
    pub fn subfolder_name(&self) -> &'static str {
        match self {
            Self::General => "General",
            Self::Documents => "Documents",
            Self::Compressed => "Compressed",
            Self::Video => "Video",
            Self::Audio => "Audio",
            Self::Images => "Images",
            Self::Programs => "Programs",
        }
    }
}

impl fmt::Display for FileCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.subfolder_name())
    }
}

impl FromStr for FileCategory {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "general" => Ok(Self::General),
            "documents" | "document" | "doc" => Ok(Self::Documents),
            "compressed" | "archive" | "archives" | "zip" => Ok(Self::Compressed),
            "video" | "videos" | "movie" => Ok(Self::Video),
            "audio" | "music" | "sound" => Ok(Self::Audio),
            "images" | "image" | "picture" | "photo" => Ok(Self::Images),
            "programs" | "program" | "apps" | "software" => Ok(Self::Programs),
            other => Err(format!("Unknown category: {other}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_category_detection_by_extension() {
        assert_eq!(
            FileCategory::from_filename_or_mime("report.pdf", None),
            FileCategory::Documents
        );
        assert_eq!(
            FileCategory::from_filename_or_mime("archive.tar.gz", None),
            FileCategory::Compressed
        );
        assert_eq!(
            FileCategory::from_filename_or_mime("movie.mkv", None),
            FileCategory::Video
        );
        assert_eq!(
            FileCategory::from_filename_or_mime("song.flac", None),
            FileCategory::Audio
        );
        assert_eq!(
            FileCategory::from_filename_or_mime("photo.png", None),
            FileCategory::Images
        );
        assert_eq!(
            FileCategory::from_filename_or_mime("setup.exe", None),
            FileCategory::Programs
        );
        assert_eq!(
            FileCategory::from_filename_or_mime("unknown.xyz", None),
            FileCategory::General
        );
    }

    #[test]
    fn test_category_detection_by_mime() {
        assert_eq!(
            FileCategory::from_filename_or_mime("file", Some("video/mp4")),
            FileCategory::Video
        );
        assert_eq!(
            FileCategory::from_filename_or_mime("file", Some("audio/mpeg")),
            FileCategory::Audio
        );
        assert_eq!(
            FileCategory::from_filename_or_mime("file", Some("image/jpeg")),
            FileCategory::Images
        );
        assert_eq!(
            FileCategory::from_filename_or_mime("file", Some("application/pdf")),
            FileCategory::Documents
        );
    }

    #[test]
    fn test_category_from_str() {
        assert_eq!(
            "video".parse::<FileCategory>().unwrap(),
            FileCategory::Video
        );
        assert_eq!(
            "compressed".parse::<FileCategory>().unwrap(),
            FileCategory::Compressed
        );
        assert_eq!(
            "doc".parse::<FileCategory>().unwrap(),
            FileCategory::Documents
        );
        assert!("invalid".parse::<FileCategory>().is_err());
    }
}
