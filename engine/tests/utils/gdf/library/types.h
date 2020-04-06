/*
 * Copyright 2018-2019 BlazingDB, Inc.
 *     Copyright 2018 Christian Noboa Mardini <christian@blazingdb.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef GDF_TESTS_BINARY_OPERATION_UTIL_TYPES_H
#define GDF_TESTS_BINARY_OPERATION_UTIL_TYPES_H


namespace gdf {
namespace library {

namespace helper {
void setScalar(gdf_scalar & scalar, int8_t value) { scalar.data.si08 = value; }

void setScalar(gdf_scalar & scalar, int16_t value) { scalar.data.si16 = value; }

void setScalar(gdf_scalar & scalar, int32_t value) { scalar.data.si32 = value; }

void setScalar(gdf_scalar & scalar, int64_t value) { scalar.data.si64 = value; }

// void setScalar(gdf_scalar& scalar, uint8_t value) {
//     scalar.data.ui08 = value;
// }

// void setScalar(gdf_scalar& scalar, uint16_t value) {
//     scalar.data.ui16 = value;
// }

// void setScalar(gdf_scalar& scalar, uint32_t value) {
//     scalar.data.ui32 = value;
// }

// void setScalar(gdf_scalar& scalar, uint64_t value) {
//     scalar.data.ui64 = value;
// }

void setScalar(gdf_scalar & scalar, float value) { scalar.data.fp32 = value; }

void setScalar(gdf_scalar & scalar, double value) { scalar.data.fp64 = value; }
}  // namespace helper

namespace helper {
template <gdf_dtype>
struct GdfEnumType;

template <>
struct GdfEnumType<GDF_invalid> {
	using Type = void;
};

template <>
struct GdfEnumType<GDF_INT8> {
	using Type = int8_t;
};

template <>
struct GdfEnumType<GDF_INT16> {
	using Type = int16_t;
};

template <>
struct GdfEnumType<GDF_INT32> {
	using Type = int32_t;
};

template <>
struct GdfEnumType<GDF_INT64> {
	using Type = int64_t;
};

// TODO percy noboa see upgrade to uints
//        template <>
//        struct GdfEnumType<GDF_UINT8> {
//            using Type = uint8_t;
//        };
//
//        template <>
//        struct GdfEnumType<GDF_UINT16> {
//            using Type = uint16_t;
//        };
//
//        template <>
//        struct GdfEnumType<GDF_UINT32> {
//            using Type = uint32_t;
//        };
//
//        template <>
//        struct GdfEnumType<GDF_UINT64> {
//            using Type = uint64_t;
//        };

template <>
struct GdfEnumType<GDF_FLOAT32> {
	using Type = float;
};

template <>
struct GdfEnumType<GDF_FLOAT64> {
	using Type = double;
};
}  // namespace helper

namespace helper {
template <typename T>
struct GdfDataType;

template <>
struct GdfDataType<int8_t> {
	static constexpr gdf_dtype Value = GDF_INT8;
};

template <>
struct GdfDataType<int16_t> {
	static constexpr gdf_dtype Value = GDF_INT16;
};

template <>
struct GdfDataType<int32_t> {
	static constexpr gdf_dtype Value = GDF_INT32;
};

template <>
struct GdfDataType<int64_t> {
	static constexpr gdf_dtype Value = GDF_INT64;
};

// TODO percy noboa see upgrade to uints
//        template <>
//        struct GdfDataType<uint8_t> {
//            static constexpr gdf_dtype Value = GDF_UINT8;
//        };
//
//        template <>
//        struct GdfDataType<uint16_t> {
//            static constexpr gdf_dtype Value = GDF_UINT16;
//        };
//
//        template <>
//        struct GdfDataType<uint32_t> {
//            static constexpr gdf_dtype Value = GDF_UINT32;
//        };
//
//        template <>
//        struct GdfDataType<uint64_t> {
//            static constexpr gdf_dtype Value = GDF_UINT64;
//        };

template <>
struct GdfDataType<float> {
	static constexpr gdf_dtype Value = GDF_FLOAT32;
};

template <>
struct GdfDataType<double> {
	static constexpr gdf_dtype Value = GDF_FLOAT64;
};
}  // namespace helper

template <gdf_dtype gdf_type>
using GdfEnumType = typename helper::GdfEnumType<gdf_type>::Type;

template <typename Type>
using GdfDataType = helper::GdfDataType<Type>;

template <typename Type>
void setScalar(gdf_scalar & scalar, Type value) {
	helper::setScalar(scalar, value);
}

static inline const char * getTypeName(gdf_dtype type) {
	switch(type) {
	case GDF_INT8: return "GDF_INT8";
	case GDF_INT16: return "GDF_INT16";
	case GDF_INT32: return "GDF_INT32";
	case GDF_INT64: return "GDF_INT64";
	case GDF_FLOAT32: return "GDF_FLOAT32";
	case GDF_FLOAT64: return "GDF_FLOAT64";
	case GDF_DATE32: return "GDF_DATE32";
	case GDF_DATE64: return "GDF_DATE64";
	case GDF_TIMESTAMP: return "GDF_TIMESTAMP";
	default: return "GDF_UNSUPPORTED_DTYPE";
	}
}
int8_t getScalar(int8_t, gdf_scalar * scalar) { return scalar->data.si08; }

int16_t getScalar(int16_t, gdf_scalar * scalar) { return scalar->data.si16; }

int32_t getScalar(int32_t, gdf_scalar * scalar) { return scalar->data.si32; }

int64_t getScalar(int64_t, gdf_scalar * scalar) { return scalar->data.si64; }

// uint8_t getScalar(uint8_t, gdf_scalar* scalar) {
//     return scalar->data.ui08;
// }

// uint16_t getScalar(uint16_t, gdf_scalar* scalar) {
//     return scalar->data.ui16;
// }

// uint32_t getScalar(uint32_t, gdf_scalar* scalar) {
//     return scalar->data.ui32;
// }

// uint64_t getScalar(uint64_t, gdf_scalar* scalar) {
//     return scalar->data.ui64;
// }

float getScalar(float, gdf_scalar * scalar) { return scalar->data.fp32; }

double getScalar(double, gdf_scalar * scalar) { return scalar->data.fp64; }

}  // namespace library
}  // namespace gdf

#endif
