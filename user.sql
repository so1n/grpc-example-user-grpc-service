create database if not exists grpc_example;
use grpc_example;
create table if not exists user (
    uid varchar(32) not null comment 'user id' primary key ,
    user_name varchar(128) default '' not null comment 'user name',
    password varchar(128) not null comment 'user password',
    create_time             timestamp   default CURRENT_TIMESTAMP null,
    update_time             timestamp   default CURRENT_TIMESTAMP not null,
    deleted                 tinyint(2)  default 0                 not null
) comment 'user sql table';

create table if not exists user_token (
    uid varchar(32) not null comment 'user id' primary key ,
    token varchar(128) not null comment 'user token',
    create_time             timestamp   default CURRENT_TIMESTAMP null,
    update_time             timestamp   default CURRENT_TIMESTAMP not null,
    deleted                 tinyint(2)  default 0                 not null
) comment 'user token'
