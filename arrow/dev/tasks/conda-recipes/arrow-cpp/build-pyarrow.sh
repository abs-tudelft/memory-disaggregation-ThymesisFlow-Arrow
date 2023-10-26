#!/bin/sh

set -e
set -x

# Build dependencies
export ARROW_HOME=$PREFIX
export PARQUET_HOME=$PREFIX
export SETUPTOOLS_SCM_PRETEND_VERSION=$PKG_VERSION
export PYARROW_BUILD_TYPE=release
export PYARROW_BUNDLE_ARROW_CPP_HEADERS=0
export PYARROW_WITH_DATASET=1
export PYARROW_WITH_FLIGHT=1
export PYARROW_WITH_GANDIVA=1
export PYARROW_WITH_GCS=1
export PYARROW_WITH_HDFS=1
export PYARROW_WITH_ORC=1
export PYARROW_WITH_PARQUET=1
export PYARROW_WITH_PARQUET_ENCRYPTION=1
export PYARROW_WITH_PLASMA=1
export PYARROW_WITH_S3=1
export PYARROW_CMAKE_GENERATOR=Ninja
export PYARROW_CMAKE_OPTIONS="-DARROW_SIMD_LEVEL=NONE"
BUILD_EXT_FLAGS=""

# Enable CUDA support
if [[ ! -z "${cuda_compiler_version+x}" && "${cuda_compiler_version}" != "None" ]]; then
    export PYARROW_WITH_CUDA=1
else
    export PYARROW_WITH_CUDA=0
fi

if [[ "${target_platform}" == osx-* ]]; then
    # See https://conda-forge.org/docs/maintainer/knowledge_base.html#newer-c-features-with-old-sdk
    CXXFLAGS="${CXXFLAGS} -D_LIBCPP_DISABLE_AVAILABILITY"
fi

# Limit number of threads used to avoid hardware oversubscription
if [[ "${target_platform}" == "linux-aarch64" ]] || [[ "${target_platform}" == "linux-ppc64le" ]]; then
     export CMAKE_BUILD_PARALLEL_LEVEL=4
fi

cd python

$PYTHON setup.py \
        build_ext \
        install --single-version-externally-managed \
                --record=record.txt

if [[ "$PKG_NAME" == "pyarrow" ]]; then
    rm -r ${SP_DIR}/pyarrow/tests
fi