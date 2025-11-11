# Claude Code Instructions for GPU Port Collectors

## Project Constitution

**IMPORTANT**: This project follows the GPU Port constitution defined in `.specify/memory/constitution.md`.

Before implementing any feature or making architectural decisions, you MUST review and follow the constitution, which includes:

### Key Principles from Constitution:

1. **Python 3.12+ with async/await**: All I/O operations must use async/await patterns
2. **Multi-Provider Consistency**: All providers must return the same GPUInstance schema
3. **API-First Architecture**: Prioritize official provider APIs over browser automation
4. **Data Quality**: Validate all data with Pydantic, include timestamps
5. **Error Handling**: One provider failure must not block others
6. **Type Safety**: Pass mypy strict mode with zero errors
7. **Test Coverage**: Maintain 80%+ test coverage
8. **Open Source**: Apache 2.0 license, community-friendly contribution guide

### Implementation Guidelines:

- Read the full constitution at `.specify/memory/constitution.md` before starting any task
- When in doubt, refer back to constitution principles
- All code must align with constitutional requirements
- Breaking constitution rules requires explicit discussion and approval

### Project Structure:

This is the **collectors** repository - an open-source framework for collecting GPU pricing and availability data from cloud providers (RunPod, Lambda Labs, Vast.ai, etc.).

Related repositories:
- `gpuport/cli` - Command line interface
