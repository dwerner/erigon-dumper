// Exact line-by-line port of Go's compress.go and parallel_compress.go
// This is the main entry point that ties together all the ported functions

use std::path::{Path, PathBuf};
use std::fs::File;
use std::io::{Write, BufWriter};
use crate::error::{Result, Error};
use crate::compress_go_port::{Pattern, cover_word_by_patterns};
use crate::compress_go_port2::{RawWordsFile, compress_with_pattern_candidates, Cfg as InternalCfg};
use crate::dictionary_builder::DictionaryBuilder;
use crate::patricia::PatriciaTree;

// ========== Port of compress.go Cfg struct ==========
// type Cfg struct {
//     MinPatternScore   uint64
//     MinPatternLen     int
//     MaxPatternLen     int
//     MaxDictPatterns   int
//     MaxWorkers        int
//     // ... other fields
// }
#[derive(Clone, Debug)]
pub struct Cfg {
    pub min_pattern_score: u64,
    pub min_pattern_len: usize,
    pub max_pattern_len: usize,
    pub max_dict_patterns: usize,
    pub workers: usize,
}

impl Default for Cfg {
    fn default() -> Self {
        // DefaultCfg from Go
        Cfg {
            min_pattern_score: 100,
            min_pattern_len: 5,
            max_pattern_len: 128,
            max_dict_patterns: 64 * 1024,
            workers: 1,
        }
    }
}

// ========== Port of compress.go Compressor struct ==========
// type Compressor struct {
//     ctx                context.Context
//     wg                 sync.WaitGroup
//     suffixCollectors   [BtreeStartBlockSize]*etl.Collector
//     uncompressedFile   *RawWordsFile
//     tmpDir             string
//     logPrefix          string
//     outputFile         string
//     tmpOutFilePath     string
//     wordsCount         uint64
//     cfg                Cfg
//     lvl                log.Lvl
//     logger             log.Logger
// }
pub struct Compressor {
    pub uncompressed_file: RawWordsFile,
    pub tmp_dir: PathBuf,
    pub log_prefix: String,
    pub output_file: PathBuf,
    pub tmp_out_file_path: PathBuf,
    pub words_count: u64,
    pub cfg: Cfg,
    pub dict_builder: DictionaryBuilder,
}

// ========== Port of compress.go NewCompressor ==========
// func NewCompressor(ctx context.Context, logPrefix, outputFile, tmpDir string, cfg Cfg, lvl log.Lvl, logger log.Logger) (*Compressor, error) {
impl Compressor {
    pub fn new(log_prefix: &str, output_file: &Path, tmp_dir: &Path, cfg: Cfg) -> Result<Self> {
        // uncompressedFile, err := NewRawWordsFile(filepath.Join(tmpDir, "uncompressed"))
        let uncompressed_path = tmp_dir.join("uncompressed");
        let uncompressed_file = RawWordsFile::new(&uncompressed_path.to_string_lossy())?;
        
        // tmpOutFilePath := outputFile + ".tmp"
        let tmp_out_file_path = PathBuf::from(format!("{}.tmp", output_file.display()));
        
        // dictBuilder := NewDictionaryBuilder(cfg.MaxDictPatterns)
        let dict_builder = DictionaryBuilder::new(cfg.max_dict_patterns);
        
        // return &Compressor{
        Ok(Compressor {
            // uncompressedFile: uncompressedFile,
            uncompressed_file,
            // tmpDir:           tmpDir,
            tmp_dir: tmp_dir.to_path_buf(),
            // logPrefix:        logPrefix,
            log_prefix: log_prefix.to_string(),
            // outputFile:       outputFile,
            output_file: output_file.to_path_buf(),
            // tmpOutFilePath:   tmpOutFilePath,
            tmp_out_file_path,
            // wordsCount:       0,
            words_count: 0,
            // cfg:              cfg,
            cfg,
            // dictBuilder
            dict_builder,
        // }, nil
        })
    }

    // ========== Port of compress.go AddWord ==========
    // func (c *Compressor) AddWord(word []byte) error {
    pub fn add_word(&mut self, word: &[u8]) -> Result<()> {
        // c.wordsCount++
        self.words_count += 1;
        
        // if err := c.dictBuilder.AddWord(word); err != nil {
        //     return err
        // }
        self.dict_builder.add_word(word).map_err(|e| Error::InvalidFormat(e))?;
        
        // return c.uncompressedFile.Append(word)
        self.uncompressed_file.append(word)
    }

    // ========== Port of compress.go AddUncompressedWord ==========
    // func (c *Compressor) AddUncompressedWord(word []byte) error {
    pub fn add_uncompressed_word(&mut self, word: &[u8]) -> Result<()> {
        // c.wordsCount++
        self.words_count += 1;
        // return c.uncompressedFile.AppendUncompressed(word)
        self.uncompressed_file.append_uncompressed(word)
    }

    // ========== Port of compress.go Compress ==========
    // func (c *Compressor) Compress() error {
    pub fn compress(&mut self) -> Result<()> {
        // if err := c.uncompressedFile.Flush(); err != nil {
        //     return err
        // }
        self.uncompressed_file.flush()?;
        
        // c.dictBuilder.Finish()
        self.dict_builder.finish();
        // c.dictBuilder.Sort()
        self.dict_builder.sort();
        // c.dictBuilder.Limit()
        self.dict_builder.limit();
        
        // logEvery := time.NewTicker(20 * time.Second)
        // defer logEvery.Stop()
        
        // if c.lvl < log.LvlTrace {
        //     c.logger.Log(c.lvl, fmt.Sprintf("[%s] BuildDict", c.logPrefix), "workers", c.cfg.MaxWorkers)
        // }
        
        // t := time.Now()
        
        // Build patricia tree from dictionary
        // var pt patricia.PatriciaTree
        let mut pt = PatriciaTree::new();
        // code2pattern := make([]*Pattern, 0, 256)
        let mut code2pattern: Vec<Pattern> = Vec::with_capacity(256);
        
        // dictBuilder.ForEach(func(score uint64, word []byte) {
        self.dict_builder.for_each(|score, word| {
            // Check min_pattern_score like Go does
            if score < self.cfg.min_pattern_score {
                return;
            }
            // p := &Pattern{
            let p = Pattern {
                // score:    score,
                score,
                // uses:     0,
                uses: 0,
                // code:     uint64(len(code2pattern)),
                code: code2pattern.len() as u64,
                // codeBits: 0,
                code_bits: 0,
                // word:     word,
                word: word.to_vec(),
                depth: 0,
            // }
            };
            // pt.Insert(word, p)
            pt.insert(word, code2pattern.len());
            // code2pattern = append(code2pattern, p)
            code2pattern.push(p);
        // })
        });
        
        // Create output file
        // cf, err := os.Create(c.tmpOutFilePath)
        // if err != nil {
        //     return err
        // }
        let mut cf = File::create(&self.tmp_out_file_path)?;
        // defer cf.Close()
        
        // Convert cfg
        let internal_cfg = InternalCfg {
            workers: self.cfg.workers,
            min_pattern_score: self.cfg.min_pattern_score,
            min_pattern_len: self.cfg.min_pattern_len,
            max_pattern_len: self.cfg.max_pattern_len,
            max_dict_patterns: self.cfg.max_dict_patterns,
        };
        
        // err = compressWithPatternCandidates(c.ctx, false, c.cfg, c.logPrefix, c.tmpOutFilePath, cf, c.uncompressedFile, c.dictBuilder, c.lvl, c.logger)
        compress_with_pattern_candidates(
            false, // trace
            &internal_cfg,
            &self.tmp_out_file_path,
            &mut cf,
            &self.uncompressed_file,
            &code2pattern,
            &pt,
        )?;
        
        // if err != nil {
        //     return err
        // }
        
        // if err = cf.Close(); err != nil {
        //     return err
        // }
        drop(cf);
        
        // if err = os.Rename(c.tmpOutFilePath, c.outputFile); err != nil {
        //     return fmt.Errorf("renaming: %w", err)
        // }
        std::fs::rename(&self.tmp_out_file_path, &self.output_file)?;
        
        // return nil
        Ok(())
    }

    // ========== Port of compress.go Close ==========
    // func (c *Compressor) Close() {
    pub fn close(mut self) -> Result<()> {
        // c.uncompressedFile.CloseAndRemove()
        self.uncompressed_file.close_and_remove()?;
        // c.dictBuilder.Close()
        self.dict_builder.close();
        Ok(())
    }
}

// Re-export functions that tests might use
pub fn compress_empty_dict(data: &[u8], output_path: &Path) -> Result<()> {
    let tmp_dir = output_path.parent().unwrap();
    let mut cfg = Cfg::default();
    cfg.min_pattern_score = u64::MAX; // No patterns
    
    let mut compressor = Compressor::new("compress_empty_dict", output_path, tmp_dir, cfg)?;
    compressor.add_word(data)?;
    compressor.compress()?;
    compressor.close()?;
    
    Ok(())
}