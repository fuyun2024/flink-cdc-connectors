-- Copyright 2022 Ververica Inc.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--   http://www.apache.org/licenses/LICENSE-2.0
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  column_type_test
-- ----------------------------------------------------------------------------------------------------------------

create table DEBEZIUM.FULL_TYPES (
    id numeric(9,0) not null,
    val_varchar varchar(1000),
    val_varchar2 varchar2(1000),
    val_nvarchar2 nvarchar2(1000),
    val_char char(3),
    val_nchar nchar(3),
    val_bf binary_float,
    val_bd binary_double,
    val_f float,
    val_f_10 float (10),
    val_num number(10,6),
    val_dp double precision,
    val_r real,
    val_decimal decimal(10, 6),
    val_numeric numeric(10, 6),
    val_num_vs number,
    val_int int,
    val_integer integer,
    val_smallint smallint,
    val_number_38_no_scale number(38),
    val_number_38_scale_0 number(38, 0),
    val_number_1 number(1),
    val_number_2 number(2),
    val_number_4 number(4),
    val_number_9 number(9),
    val_number_18 number(18),
    val_number_2_negative_scale number(1, -1),
    val_number_4_negative_scale number(2, -2),
    val_number_9_negative_scale number(8, -1),
    val_number_18_negative_scale number(16, -2),
    val_number_36_negative_scale number(36, -2),
    val_date date,
    val_ts timestamp,
    val_ts_precision2 timestamp(2),
    val_ts_precision4 timestamp(4),
    val_ts_precision9 timestamp(6),
    val_tstz timestamp with time zone,
    val_tsltz timestamp with local time zone,
    val_int_ytm interval year to month,
    val_int_dts interval day(3) to second(2),
    val_clob_inline clob,
    val_nclob_inline nclob,
    location sdo_geometry,
    primary key (id)
);

INSERT INTO DEBEZIUM.FULL_TYPES VALUES (1, 'vč2', 'vč2', 'nvč2', 'c', 'nč',
                               1.1, 2.22, 3.33, 8.888, 4.4444, 5.555, 6.66, 1234.567891, 1234.567891, 77.323,
                               1, 22, 333, 4444, 5555, 1, 99, 9999, 999999999, 999999999999999999, 94, 9949, 999999994,
                               999999999999999949, 99999999999999999999999999999999999949,
                               TO_DATE('2022-10-30', 'yyyy-mm-dd'),
                               TO_TIMESTAMP('2022-10-30 12:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5'),
                               TO_TIMESTAMP('2022-10-30 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),
                               TO_TIMESTAMP('2022-10-30 12:34:56.12545', 'yyyy-mm-dd HH24:MI:SS.FF5'),
                               TO_TIMESTAMP('2022-10-30 12:34:56.125456789', 'yyyy-mm-dd HH24:MI:SS.FF9'),
                               TO_TIMESTAMP_TZ('2022-10-30 01:34:56.00789 -11:00', 'yyyy-mm-dd HH24:MI:SS.FF5 TZH:TZM'),
                               TO_TIMESTAMP_TZ('2022-10-30 01:34:56.00789', 'yyyy-mm-dd HH24:MI:SS.FF5'),
                               INTERVAL '-3-6' YEAR TO MONTH,
                               INTERVAL '-1 2:3:4.56' DAY TO SECOND,
                               TO_CLOB ('col_clob'),
                               TO_NCLOB ('col_nclob'),
                               SDO_GEOMETRY(2003, NULL, NULL, SDO_ELEM_INFO_ARRAY(1, 1003, 3), SDO_ORDINATE_ARRAY(1, 1, 5, 7))
                               );
