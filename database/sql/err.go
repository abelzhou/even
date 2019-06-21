/**
 *  author:Abel
 *  email:abel.zhou@hotmail.com
 *  date:2019-06-12
 */
package sql

import "errors"

var ERR_NOPREPARED  = errors.New("SQL was nil.Please using prepared function.")

var ERR_TOOMANEYCOLUMNS = errors.New("Too many columns.")

var ERR_MUSTNOTBESLICE = errors.New("Must not be a slice.")

var ERR_MUSTBESLICE = errors.New("Must be a slice.")

var ERR_MUSTBEPOINTER = errors.New("Must be a pointer.")