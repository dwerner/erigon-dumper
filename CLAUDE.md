## Rust Development Practices

### Building
- ALWAYS use cargo to build, don't revert to using rustc directly for testing
- ALWAYS run `cargo fmt --all` after changes to ensure formatting is consistent
- NEVER create examples or new binary targets unless directed to do so

### Interior Mutability
- Whenever using Mutex, Arc, or RefCell, ALWAYS note in struct doc comments what we're intending with internal or external mutability
- When making notes about internal mutability, refer to that not what primitive we are using for it

### Error Handling
- NEVER use type aliases for Result types (e.g., type Result<T> = std::result::Result<T, Error>)
- Always use the full std::result::Result<T, Error> type in function signatures
- Use thiserror for error enums

### Code Style Warnings
- DON'T 'fix' warnings by underscore (_) prefixing variables. It indicates things are going unimplemented
- Leave unused variables as-is - they often indicate unimplemented functionality

### Async/Threading
- Don't use the std::thread api - use futures/async/await api instead (rare exceptions should be confirmed)
- Library crates MUST work with any async runtime (Tokio, async-std, smol)
- Use async-process, async-fs, async-net instead of runtime-specific alternatives
- When writing async unit tests, use smol_potat::test annotation

# important-instruction-reminders
Do what has been asked; nothing more, nothing less.
NEVER create files unless they're absolutely necessary for achieving your goal.
ALWAYS prefer editing an existing file to creating a new one.
NEVER proactively create documentation files (*.md) or README files. Only create documentation files if explicitly requested by the User.