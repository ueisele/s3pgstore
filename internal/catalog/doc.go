// Package catalog holds the schema constants, DDL fragment
// templates, and shared SQL strings used by the s3pgstore root
// package and every subpackage / cmd binary that touches the
// catalog tables. Centralized here so table names and core SQL
// can't drift between the runtime path and operator tooling.
package catalog
