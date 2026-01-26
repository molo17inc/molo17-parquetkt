/*
 * Copyright 2026 MOLO17
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


package com.molo17.parquetkt.format

object ParquetConstants {
    const val MAGIC = "PAR1"
    val MAGIC_BYTES = MAGIC.toByteArray(Charsets.US_ASCII)
    const val MAGIC_LENGTH = 4
    const val FOOTER_LENGTH_SIZE = 4
    
    const val DEFAULT_PAGE_SIZE = 1024 * 1024 // 1 MB
    const val DEFAULT_ROW_GROUP_SIZE = 128 * 1024 * 1024 // 128 MB
    const val DEFAULT_DICTIONARY_PAGE_SIZE = 1024 * 1024 // 1 MB
    
    const val VERSION = 1
}
