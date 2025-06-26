cmake_minimum_required(VERSION 3.10)
project(minidb C)

# 设置 C 标准
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# 设置输出目录
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# 设置调试构建类型
set(CMAKE_BUILD_TYPE Debug)
# 或者提供默认值（只在未指定时生效）
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()


# 查找 zlib 依赖
find_package(ZLIB REQUIRED)

# 包含目录
include_directories(
    include
    include/server
    ${ZLIB_INCLUDE_DIRS}
)

# ================== 核心库 ==================
add_library(minidb_core STATIC
    src/minidb.c
    src/txmgr.c
    src/wal.c
    src/catalog.c
    src/page.c
   src/tuple.c
    src/lock.c
   src/server/server.c
   src/server/executor.c
   src/server/sql_exec.c
   src/server/parser.c
   #src/client/client.c

)

target_link_libraries(minidb_core ${ZLIB_LIBRARIES})

# ================== 主程序 ==================
add_executable(minidb
    src/main.c
)

target_link_libraries(minidb minidb_core pthread)

# ================== 测试程序 ==================
add_executable(test_minidb
#    test/test_minidb.c
#    test/test_txmgr.c
#    test/test_wal.c
#    test/test_page.c
#    test/test_tuple.c
    test/test_users.c

)

target_link_libraries(test_minidb minidb_core)
#target_link_libraries(minidb_core)

# ================== 安装目标 ==================
install(TARGETS minidb DESTINATION bin)
install(DIRECTORY include/ DESTINATION include/minidb)

# ================== 测试配置 ==================
#enable_testing()
#add_test(NAME test_minidb COMMAND test_minidb)

# ================== 可选：代码格式化 ==================
find_program(CLANG_FORMAT "clang-format")
if(CLANG_FORMAT)
    add_custom_target(format
        COMMAND ${CLANG_FORMAT} -i
            include/*.h
            src/*.c
            test/*.c
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Formatting source files..."
    )
endif()

# ================== 文档生成 ==================
find_package(Doxygen)
if(DOXYGEN_FOUND)
    set(DOXYGEN_INPUT 
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/src
    )
    set(DOXYGEN_OUTPUT_DIR ${CMAKE_BINARY_DIR}/docs)
    
    configure_file(
        ${CMAKE_SOURCE_DIR}/Doxyfile.in
        ${CMAKE_BINARY_DIR}/Doxyfile
        @ONLY
    )
    
    add_custom_target(docs
        COMMAND ${DOXYGEN_EXECUTABLE} ${CMAKE_BINARY_DIR}/Doxyfile
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Generating API documentation with Doxygen"
        VERBATIM
    )
endif()

# ================== 打包配置 ==================
set(CPACK_PACKAGE_NAME "minidb")
set(CPACK_PACKAGE_VERSION "1.0.0")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "A mini database with transaction support")
set(CPACK_PACKAGE_VENDOR "Your Name")
set(CPACK_PACKAGE_CONTACT "your.email@example.com")
set(CPACK_DEBIAN_PACKAGE_DEPENDS "zlib1g-dev")
set(CPACK_RPM_PACKAGE_REQUIRES "zlib-devel")
include(CPack)