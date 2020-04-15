

cmake_minimum_required(VERSION 3.4.0)

project(moj_redis VERSION 1.0.0)

add_executable(red main.c)


target_link_libraries(red hiredis m)

