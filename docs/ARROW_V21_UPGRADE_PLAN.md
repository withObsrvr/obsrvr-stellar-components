# Apache Arrow v21 Upgrade & Backend Integration Fix Plan

## Executive Summary

This document outlines a comprehensive plan to fix the current backend integration issues and upgrade obsrvr-stellar-components from Apache Arrow v17.0.0 to v21.0.0. The plan addresses compilation errors, API compatibility issues, and ensures long-term maintainability.

## Current Status Analysis

### ✅ Working Components
- **Nix Development Environment**: Fully functional with visual indicators
- **Schema Module**: Builds successfully, foundation is solid
- **Project Structure**: Well-organized component architecture
- **Backend Architecture**: Conceptually correct implementation

### ❌ Issues to Resolve

#### 1. Arrow Version Gap
- **Current**: Apache Arrow v17.0.0 (released ~2023)
- **Latest**: Apache Arrow v21.0.0 (released July 2025)
- **Gap**: 4 major versions behind with significant API changes

#### 2. Compilation Errors by Component

**stellar-arrow-source (8 errors)**:
- Flight Server interface incompatibility
- Missing `xdrProcessor` field after refactoring
- XDR API changes in Stellar protocol-23
- Arrow RecordBuilder API changes

**ttp-arrow-processor (10+ errors)**:
- Arrow compute context missing imports
- Flight path handling API changes
- IPC schema serialization changes

**arrow-analytics-sink (10+ errors)**:
- Duplicate type declarations
- Parquet writer API overhaul
- Memory allocator interface changes

#### 3. Stellar Protocol-23 Integration
- Backend API compatibility issues
- XDR structure changes
- DataStore configuration updates

## Implementation Plan

### Phase 1: Arrow v21 Upgrade Foundation (Priority: Critical)

#### 1.1 Update Nix Environment
**Estimated Time**: 2-4 hours

```bash
# Update flake.nix
- arrow-cpp: 17.0.0 → 21.0.0
- Update build dependencies
- Verify CGO compatibility
```

**Files to Update**:
- `flake.nix`: Arrow C++ library version
- All `go.mod` files: `github.com/apache/arrow/go/v17` → `github.com/apache/arrow/go/v21`

**Testing**:
```bash
nix develop --command go version
nix develop --command pkg-config --modversion arrow
```

#### 1.2 Schema Module Migration
**Estimated Time**: 1-2 hours

**Changes Required**:
- Update import paths to `v21`
- Fix `RecordBuilder.NumFields()` → `RecordBuilder.NumField()`
- Update metadata creation APIs
- Test schema compatibility

**Validation**:
```bash
cd schemas && go build -o ../build/schema-tools .
```

### Phase 2: Core API Compatibility Fixes (Priority: High)

#### 2.1 stellar-arrow-source Fixes
**Estimated Time**: 6-8 hours

**2.1.1 Flight Server Interface**
```go
// Before (v17)
type FlightServer struct {
    flight.BaseFlightServer
}

// After (v21) - Implement required methods
func (s *FlightServer) Addr() net.Addr { ... }
func (s *FlightServer) DoAction(ctx context.Context, action *flight.Action) (*flight.Result, error) { ... }
```

**2.1.2 XDR Converter Updates**
```go
// Fix RecordBuilder API
- c.builder.NumFields() → c.builder.NumField()
- Update field access patterns
- Fix array builder methods
```

**2.1.3 Service Refactoring**
```go
// Add missing xdrProcessor field
type StellarSourceService struct {
    config       *Config
    backendClient *StellarBackendClient
    xdrProcessor  *XDRProcessor  // Add this back
    // ... other fields
}
```

**2.1.4 Configuration Updates**
```go
// Add missing ConcurrentReads field
type Config struct {
    // ... existing fields
    ConcurrentReaders int `mapstructure:"concurrent_readers"`
}
```

#### 2.2 ttp-arrow-processor Fixes
**Estimated Time**: 4-6 hours

**2.2.1 Compute Engine Updates**
```go
// Update compute imports and context
import (
    "github.com/apache/arrow/go/v21/arrow/compute"
)

// Fix compute context initialization
ctx := compute.NewDefaultFunctionRegistry()
```

**2.2.2 Flight Server Path Handling**
```go
// Update path handling for v21
func (s *FlightServer) GetFlightInfo(ctx context.Context, in *flight.FlightDescriptor) (*flight.FlightInfo, error) {
    path := strings.Join(in.Path, "/")  // Convert []string to string
    // ... rest of implementation
}
```

#### 2.3 arrow-analytics-sink Fixes
**Estimated Time**: 4-6 hours

**2.3.1 Remove Duplicate Declarations**
```go
// Remove duplicate RetentionPolicy in main.go
// Keep only the one in data_lifecycle.go
```

**2.3.2 Parquet Writer API Updates**
```go
// Update to v21 parquet APIs
import "github.com/apache/arrow/go/v21/parquet/pqarrow"

// Fix WriterProperties
props := pqarrow.DefaultWriterProperties()
props.SetCompression(pqarrow.CompressionCodec_SNAPPY)
```

**2.3.3 Memory Allocator Updates**
```go
// Remove deprecated Destroy() calls
// Arrow v21 uses automatic garbage collection
```

### Phase 3: Stellar Protocol-23 Backend Integration (Priority: High)

#### 3.1 Backend Manager API Fixes
**Estimated Time**: 4-6 hours

**3.1.1 RPC Backend Configuration**
```go
// Update to correct protocol-23 API
backend := ledgerbackend.NewRPCLedgerBackend(ledgerbackend.RPCLedgerBackendOptions{
    RPCServerURL: config.RPCEndpoint,
    BufferSize:   uint32(config.BufferSize),
})
```

**3.1.2 Archive Backend Updates**
```go
// Fix historyarchive.Connect API
archive, err := historyarchive.Connect(config.ArchiveURL, historyarchive.ArchiveOptions{})
```

**3.1.3 DataStore Configuration**
```go
// Update datastore creation to match protocol-23 API
store, err := datastore.NewDataStore(ctx, datastore.DataStoreConfig{
    Type: "s3",
    Params: map[string]string{
        "bucket_path": fmt.Sprintf("%s/%s", bucketName, storagePath),
        "region":      region,
    },
})
```

#### 3.2 XDR Processing Updates
**Estimated Time**: 2-4 hours

**3.2.1 LedgerCloseMeta Changes**
```go
// Handle XDR structure changes in protocol-23
switch meta.V {
case 0:
    v0 := meta.MustV0()
    txSet := v0.TxSet.MustV1()  // Handle GeneralizedTransactionSet
case 1:
    // Handle v1 structure changes
case 2:
    // Handle new v2 structure if present
}
```

**3.2.2 Transaction Result Processing**
```go
// Fix result access patterns
if ops, err := result.Result.OperationResults(); err == nil {
    opCount += uint32(len(ops))
}
```

### Phase 4: Testing & Validation (Priority: Medium)

#### 4.1 Unit Testing
**Estimated Time**: 3-4 hours

```bash
# Test each component individually
cd schemas && go test -v ./...
cd components/stellar-arrow-source/src && go test -v ./...
cd components/ttp-arrow-processor/src && go test -v ./...
cd components/arrow-analytics-sink/src && go test -v ./...
```

#### 4.2 Integration Testing
**Estimated Time**: 2-3 hours

```bash
# Test Nix build process
nix develop --command build-all

# Test component communication
./build/stellar-arrow-source &
./build/ttp-arrow-processor &
./build/arrow-analytics-sink &

# Verify Arrow Flight connectivity
```

#### 4.3 Backend Testing
**Estimated Time**: 4-6 hours

```bash
# Test each backend type
export STELLAR_ARROW_SOURCE_BACKEND_TYPE=rpc
./build/stellar-arrow-source

export STELLAR_ARROW_SOURCE_BACKEND_TYPE=archive
./build/stellar-arrow-source

# Test datalake compatibility
export STELLAR_ARROW_SOURCE_SOURCE_TYPE=datalake
export STELLAR_ARROW_SOURCE_LEDGERS_PER_FILE=64
./build/stellar-arrow-source
```

### Phase 5: Documentation & Deployment (Priority: Low)

#### 5.1 Update Documentation
**Estimated Time**: 2-3 hours

- Update `CLAUDE.md` with Arrow v21 requirements
- Update component specifications
- Update build instructions
- Create migration guide for users

#### 5.2 Update Deployment Configurations
**Estimated Time**: 1-2 hours

- Update Docker base images
- Update Kubernetes manifests
- Update CI/CD pipeline
- Update bundle specifications

## Implementation Timeline

### Week 1: Foundation & Core Fixes
- **Day 1-2**: Phase 1 (Arrow v21 upgrade)
- **Day 3-5**: Phase 2 (API compatibility fixes)

### Week 2: Backend Integration & Testing
- **Day 6-8**: Phase 3 (Stellar backend fixes)
- **Day 9-10**: Phase 4 (Testing & validation)

### Week 3: Polish & Documentation
- **Day 11-12**: Phase 5 (Documentation & deployment)
- **Day 13-15**: Buffer time & refinements

**Total Estimated Time**: 35-50 hours over 2-3 weeks

## Risk Assessment & Mitigation

### High Risk Items
1. **Arrow v21 Breaking Changes**: Significant API overhaul
   - **Mitigation**: Incremental upgrade with comprehensive testing
   - **Fallback**: Keep v17 branch as backup

2. **Stellar Protocol-23 Stability**: Experimental branch
   - **Mitigation**: Pin to specific commit hash
   - **Fallback**: Document known limitations

3. **Complex Integration Testing**: Multiple components and backends
   - **Mitigation**: Automated test suite
   - **Fallback**: Manual validation checklist

### Medium Risk Items
1. **Nix Environment Compatibility**: Arrow v21 in Nix
   - **Mitigation**: Test on multiple platforms
   - **Fallback**: Container-based development

2. **Performance Regression**: New APIs may be slower
   - **Mitigation**: Benchmark before/after
   - **Fallback**: Performance optimization phase

## Success Criteria

### Minimum Viable Product (MVP)
- [ ] All components compile without errors
- [ ] Basic RPC backend functionality works
- [ ] Arrow Flight communication functional
- [ ] Schemas process correctly

### Full Success
- [ ] All backend types functional (RPC, Archive, Captive, Storage, Datalake)
- [ ] TTP-processor-demo compatibility maintained
- [ ] Performance equal or better than v17
- [ ] Comprehensive test suite passes
- [ ] Documentation updated and accurate

### Excellence
- [ ] Performance improvements from Arrow v21 realized
- [ ] New Arrow v21 features utilized
- [ ] Automated CI/CD pipeline functional
- [ ] Production deployment ready

## Dependencies & Prerequisites

### External Dependencies
- Apache Arrow v21.0.0 release availability in Nix
- Stellar Go SDK protocol-23 branch stability
- Go 1.23+ compatibility with Arrow v21

### Internal Dependencies
- Nix development environment functional ✅
- Project structure and architecture complete ✅
- Basic component framework in place ✅

### Team Requirements
- Go development experience (intermediate to advanced)
- Apache Arrow familiarity (basic understanding sufficient)
- Stellar blockchain knowledge (basic understanding sufficient)
- Nix package management (basic understanding sufficient)

## Post-Implementation Monitoring

### Key Metrics to Track
1. **Build Success Rate**: Should be 100% after completion
2. **Component Startup Time**: Should be ≤ 30 seconds
3. **Memory Usage**: Should be ≤ 1GB per component
4. **Processing Throughput**: Should be ≥ current performance
5. **Error Rate**: Should be < 1% for normal operations

### Monitoring Tools
- Prometheus metrics for performance tracking
- Health check endpoints for component status
- Log aggregation for error tracking
- Arrow Flight metrics for communication health

## Conclusion

This plan provides a structured approach to resolving the current compilation issues while upgrading to the latest Apache Arrow version. The phased approach ensures minimal risk while maximizing the benefits of the upgrade.

The key to success will be:
1. **Methodical execution** of each phase
2. **Comprehensive testing** at each step
3. **Documentation** of changes and decisions
4. **Performance validation** to ensure improvements

Upon completion, obsrvr-stellar-components will be:
- ✅ Built on the latest Apache Arrow v21.0.0
- ✅ Compatible with Stellar protocol-23
- ✅ Fully functional across all backend types
- ✅ Production-ready with comprehensive testing
- ✅ Well-documented and maintainable

---

**Next Steps**: Begin with Phase 1 (Arrow v21 upgrade foundation) and proceed methodically through each phase, validating success criteria at each step.