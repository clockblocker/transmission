foreach(P create edit remote show)
    add_executable(${TR_NAME}-${P})

    target_sources(${TR_NAME}-${P}
        PRIVATE
            ${P}.cc)

    target_link_libraries(${TR_NAME}-${P}
        PRIVATE
            ${TR_NAME}
            CURL::libcurl
            transmission::fmt-header-only
            libevent::core)

    tr_win32_app_info(${TR_NAME}-${P}
        "Transmission Utility ('${P}')"
        "${TR_NAME}-${P}"
        "${TR_NAME}-${P}.exe")

    install(
        TARGETS ${TR_NAME}-${P}
        DESTINATION ${CMAKE_INSTALL_BINDIR})

    if(INSTALL_DOC)
        install(
            FILES ${TR_NAME}-${P}.1
            DESTINATION ${CMAKE_INSTALL_MANDIR}/man1)
    endif()
endforeach()

add_executable(${TR_NAME}-rpc-schema)

target_sources(${TR_NAME}-rpc-schema
    PRIVATE
        rpc-schema.cc)

target_include_directories(${TR_NAME}-rpc-schema
    PRIVATE
        ${PROJECT_SOURCE_DIR})

target_link_libraries(${TR_NAME}-rpc-schema
    PRIVATE
        reflectcpp)

tr_win32_app_info(${TR_NAME}-rpc-schema
    "Transmission JSON-RPC Schema Generator"
    "${TR_NAME}-rpc-schema"
    "${TR_NAME}-rpc-schema.exe")

install(
    TARGETS ${TR_NAME}-rpc-schema
    DESTINATION ${CMAKE_INSTALL_BINDIR})
