//use postgres::types::{IsNull, Type, ToSql};
use crate::abcd_fields::AbcdField;
use crate::abcd_parser::AbcdResult;
use crate::abcd_parser::NumericMap;
use crate::abcd_parser::TextualMap;
use crate::settings;
use failure::Error;
use log::debug;
use postgres::params::ConnectParams;
use postgres::params::Host;
use postgres::tls::openssl::OpenSsl;
use postgres::{Connection, TlsMode};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use csv::WriterBuilder;

/// A postgresql database DAO for storing datasets.
pub struct DatabaseSink<'s> {
    connection: Connection,
    database_settings: &'s settings::Database,
    dataset_numeric_fields: Vec<String>,
    dataset_numeric_fields_hash: Vec<String>,
    dataset_textual_fields: Vec<String>,
    dataset_textual_fields_hash: Vec<String>,
    inserted_datasets: HashMap<String, u32>,
    next_dataset_id: u32,
    unit_numeric_fields: Vec<String>,
    unit_numeric_fields_hash: Vec<String>,
    unit_textual_fields: Vec<String>,
    unit_textual_fields_hash: Vec<String>,
}

impl<'s> DatabaseSink<'s> {
    pub fn new(database_settings: &'s settings::Database, abcd_fields: &HashMap<Vec<u8>, AbcdField>) -> Result<Self, Error> {
        let negotiator = if database_settings.tls {
            Some(OpenSsl::new()?)
        } else {
            None
        };
        let params = ConnectParams::builder()
            .user(&database_settings.user, Some(&database_settings.password))
            .port(database_settings.port)
            .database(&database_settings.database)
            .build(Host::Tcp(database_settings.host.clone()));

        let mut hasher = sha1::Sha1::new();

        let dataset_numeric_fields = abcd_fields.values()
            .filter(|field| field.global_field && field.numeric)
            .map(|field| field.field.clone())
            .collect::<Vec<String>>();
        let dataset_numeric_fields_hash = dataset_numeric_fields.iter()
            .map(|field| {
                hasher.reset();
                hasher.update(field.as_bytes());
                hasher.digest().to_string()
            })
            .collect::<Vec<String>>();
        let dataset_textual_fields = abcd_fields.values()
            .filter(|field| field.global_field && !field.numeric)
            .map(|field| field.field.clone())
            .collect::<Vec<String>>();
        let dataset_textual_fields_hash = dataset_textual_fields.iter()
            .map(|field| {
                hasher.reset();
                hasher.update(field.as_bytes());
                hasher.digest().to_string()
            })
            .collect::<Vec<String>>();
        let unit_numeric_fields = abcd_fields.values()
            .filter(|field| !field.global_field && field.numeric)
            .map(|field| field.field.clone())
            .collect::<Vec<String>>();
        let unit_numeric_fields_hash = unit_numeric_fields.iter()
            .map(|field| {
                hasher.reset();
                hasher.update(field.as_bytes());
                hasher.digest().to_string()
            })
            .collect::<Vec<String>>();
        let unit_textual_fields = abcd_fields.values()
            .filter(|field| !field.global_field && !field.numeric)
            .map(|field| field.field.clone())
            .collect::<Vec<String>>();
        let unit_textual_fields_hash = unit_textual_fields.iter()
            .map(|field| {
                hasher.reset();
                hasher.update(field.as_bytes());
                hasher.digest().to_string()
            })
            .collect::<Vec<String>>();

        let mut sink = Self {
            connection: Connection::connect(
                params,
                if let Some(negotiator) = &negotiator {
                    TlsMode::Prefer(negotiator)
                } else {
                    TlsMode::None
                },
            )?,
            database_settings,
            dataset_numeric_fields,
            dataset_numeric_fields_hash,
            dataset_textual_fields,
            dataset_textual_fields_hash,
            inserted_datasets: HashMap::new(),
            next_dataset_id: 1,
            unit_numeric_fields,
            unit_numeric_fields_hash,
            unit_textual_fields,
            unit_textual_fields_hash,
        };

        sink.initialize_schema(abcd_fields)?;

        Ok(sink)
    }

    fn initialize_schema(&mut self, abcd_fields: &HashMap<Vec<u8>, AbcdField>) -> Result<(), Error> {
        // drop temp tables if they exist
        self.connection.execute(&format!(
            "DROP TABLE IF EXISTS {};", &self.database_settings.temp_unit_table
        ), &mut [])?;
        self.connection.execute(&format!(
            "DROP TABLE IF EXISTS {};", &self.database_settings.temp_dataset_table
        ), &mut [])?;
        self.connection.execute(&format!(
            "DROP TABLE IF EXISTS {}_translation;", &self.database_settings.temp_dataset_table
        ), &mut [])?;

        let mut fields = Vec::new();

        // create temporary dataset table
        fields.push(format!("{} int primary key", self.database_settings.dataset_id_column));
        for numeric_field in &self.dataset_numeric_fields_hash {
            let null_string = if let Some(_field) = abcd_fields.get(numeric_field.as_bytes()) {
                // TODO: enforce/filter not null
//                if field.vat_mandatory { "NOT NULL" } else { "" }
                ""
            } else {
                ""
            };
            fields.push(format!(
                "\"{}\" {} double precision", numeric_field, null_string
            ));
        }
        for textual_field in &self.dataset_textual_fields_hash {
            let null_string = if let Some(_field) = abcd_fields.get(textual_field.as_bytes()) {
                // TODO: enforce/filter not null
//                if field.vat_mandatory { "NOT NULL" } else { "" }
                ""
            } else {
                ""
            };
            fields.push(format!(
                "\"{}\" {} text", textual_field, null_string
            ));
        }

        self.connection.execute(&format!(
            "create table {} ( {} );", self.database_settings.temp_dataset_table, fields.join(",")
        ), &mut [])?;

        fields.clear();

        // create temporary unit table
        fields.push(format!("{} int not null", self.database_settings.dataset_id_column));
        for (numeric_field, hash) in self.unit_numeric_fields.iter().zip(&self.unit_numeric_fields_hash) {
            let null_string = if let Some(_field) = abcd_fields.get(numeric_field.as_bytes()) {
                // TODO: enforce/filter not null
//                if field.vat_mandatory { "NOT NULL" } else { "" }
                ""
            } else {
                ""
            };
            fields.push(format!(
                "\"{}\" double precision {}", hash, null_string
            ));
        }
        for (textual_field, hash) in self.unit_textual_fields.iter().zip(&self.unit_textual_fields_hash) {
            let null_string = if let Some(_field) = abcd_fields.get(textual_field.as_bytes()) {
                // TODO: enforce/filter not null
//                if field.vat_mandatory { "NOT NULL" } else { "" }
                ""
            } else {
                ""
            };
            fields.push(format!(
                "\"{}\" text {}", hash, null_string
            ));
        }

        self.connection.execute(&format!(
            "create table {} ( {} );", self.database_settings.temp_unit_table, fields.join(",")
        ), &mut [])?;

        // create mapping table from hash to field
        {
            self.connection.execute(&format!(
                "create table {}_translation (name text not null, hash text not null);",
                self.database_settings.temp_dataset_table
            ), &mut [])?;
            let statement = self.connection.prepare(&format!(
                "insert into {}_translation(name, hash) VALUES ($1, $2);",
                self.database_settings.temp_dataset_table
            ))?;
            for (name, hash) in self.dataset_numeric_fields.iter().zip(&self.dataset_numeric_fields_hash) {
                statement.execute(&[name, hash])?;
            }
            for (name, hash) in self.dataset_textual_fields.iter().zip(&self.dataset_textual_fields_hash) {
                statement.execute(&[name, hash])?;
            }
            for (name, hash) in self.unit_numeric_fields.iter().zip(&self.unit_numeric_fields_hash) {
                statement.execute(&[name, hash])?;
            }
            for (name, hash) in self.unit_textual_fields.iter().zip(&self.unit_textual_fields_hash) {
                statement.execute(&[name, hash])?;
            }
        }

        Ok(())
    }

    pub fn migrate_schema(&mut self) -> Result<(), Error> {
        let foreign_key_statement = format!(
            "ALTER TABLE {} ADD CONSTRAINT {}_{}_fk FOREIGN KEY ({}) REFERENCES {}({});",
            &self.database_settings.temp_unit_table,
            &self.database_settings.temp_unit_table,
            &self.database_settings.dataset_id_column,
            &self.database_settings.dataset_id_column,
            &self.database_settings.temp_dataset_table,
            &self.database_settings.dataset_id_column
        );
        debug!("{}", &foreign_key_statement);
        self.connection.execute(&foreign_key_statement, &mut [])?;

        let mut hasher = sha1::Sha1::new();
        let indexed_unit_column_names = self.database_settings.unit_indexed_columns.iter()
            .map(|field| {
                hasher.reset();
                hasher.update(field.as_bytes());
                hasher.digest().to_string()
            })
            .collect::<Vec<String>>();
        let unit_index_statement = format!(
            "CREATE INDEX {}_idx ON {} USING btree ({}, \"{}\");",
            &self.database_settings.temp_unit_table,
            &self.database_settings.temp_unit_table,
            &self.database_settings.dataset_id_column,
            indexed_unit_column_names.join("\", \"")
        );
        debug!("{}", &unit_index_statement);
        self.connection.execute(&unit_index_statement, &mut [])?;

        let cluster_statement = format!(
            "CLUSTER {}_idx ON {};",
            &self.database_settings.temp_unit_table,
            &self.database_settings.temp_unit_table
        );
        debug!("{}", &cluster_statement);
        self.connection.execute(&cluster_statement, &mut [])?;

        let datasets_analyze_statement = format!(
            "VACUUM ANALYZE {};",
            &self.database_settings.temp_dataset_table
        );
        debug!("{}", &datasets_analyze_statement);
        self.connection.execute(&datasets_analyze_statement, &mut [])?;

        let units_analyze_statement = format!(
            "VACUUM ANALYZE {};",
            &self.database_settings.temp_unit_table
        );
        debug!("{}", &units_analyze_statement);
        self.connection.execute(&units_analyze_statement, &mut [])?;

        let transaction = self.connection.transaction_with(
            postgres::transaction::Config::new()
                .isolation_level(postgres::transaction::IsolationLevel::Serializable)
                .read_only(false)
        )?;

        // delete old tables
        transaction.execute(&format!(
            "DROP TABLE IF EXISTS {};", &self.database_settings.unit_table
        ), &mut [])?;
        transaction.execute(&format!(
            "DROP TABLE IF EXISTS {};", &self.database_settings.dataset_table
        ), &mut [])?;
        transaction.execute(&format!(
            "DROP TABLE IF EXISTS {}_translation;", &self.database_settings.dataset_table
        ), &mut [])?;

        // rename temp tables
        transaction.execute(&format!(
            "ALTER TABLE {} RENAME TO {};", &self.database_settings.temp_unit_table, &self.database_settings.unit_table
        ), &mut [])?;
        transaction.execute(&format!(
            "ALTER TABLE {} RENAME TO {};", &self.database_settings.temp_dataset_table, &self.database_settings.dataset_table
        ), &mut [])?;
        // rename temp tables
        transaction.execute(&format!(
            "ALTER TABLE {}_translation RENAME TO {}_translation;", &self.database_settings.temp_dataset_table, &self.database_settings.dataset_table
        ), &mut [])?;

        // rename constraints/indexes
        transaction.execute(&format!(
            "ALTER TABLE {} RENAME CONSTRAINT {}_{}_fk TO {}_{}_fk;",
            &self.database_settings.unit_table,
            &self.database_settings.temp_unit_table, &self.database_settings.dataset_id_column,
            &self.database_settings.unit_table, &self.database_settings.dataset_id_column
        ), &mut [])?;
        transaction.execute(&format!(
            "ALTER INDEX {}_idx RENAME TO {}_idx;",
            &self.database_settings.temp_unit_table, &self.database_settings.unit_table
        ), &mut [])?;

        transaction.commit()?;

        Ok(())
    }

    // TODO: split into functions
    pub fn insert_dataset(&mut self, abcd_data: &AbcdResult) -> Result<(), Error> {
        const POSTGRES_CSV_CONFIGURATION: &str = "DELIMITER '\t', NULL '', QUOTE '\"', ESCAPE '\"', FORMAT CSV";

        // retrieve id for dataset
        // if the dataset is unseen, it is necessary to create a database entry at first
        let dataset_hash = self.to_hash_value(&abcd_data.dataset_data);
        let dataset_id = match self.inserted_datasets.entry(dataset_hash) {
            Entry::Occupied(e) => *e.get(),
            Entry::Vacant(o) => {
                let id = self.next_dataset_id;

                let (dataset_numeric_data, dataset_textual_data) = &abcd_data.dataset_data;

                let mut values = WriterBuilder::new()
                    .terminator(csv::Terminator::Any(b'\n'))
                    .delimiter(b'\t')
                    .quote(b'"')
                    .escape(b'"')
                    .has_headers(false)
                    .from_writer(vec![]);
                let mut columns: Vec<&str> = vec![self.database_settings.dataset_id_column.as_ref()];

                values.write_field(id.to_string())?;

                for (numeric_field, hash) in self.dataset_numeric_fields.iter()
                    .zip(&self.dataset_numeric_fields_hash) {
                    columns.push(&hash);
                    if let Some(&numeric_value) = dataset_numeric_data.get(numeric_field) {
                        values.write_field(numeric_value.to_string())?;
                    } else {
                        values.write_field(""/*&[]*/)?;
                    }
                }

                for (textual_field, hash) in self.dataset_textual_fields.iter()
                    .zip(&self.dataset_textual_fields_hash) {
                    columns.push(&hash);
                    if let Some(textual_value) = dataset_textual_data.get(textual_field) {
                        values.write_field(textual_value)?;
                    } else {
                        values.write_field(""/*&[]*/)?;
                    }
                }

                values.write_record(None::<&[u8]>)?; // terminate record

                let copy_statement = format!(
                    "COPY {}(\"{}\") FROM STDIN WITH ({})",
                    self.database_settings.temp_dataset_table, columns.join("\",\""),
                    POSTGRES_CSV_CONFIGURATION
                );

                let value_string = values.into_inner()?;

//                dbg!(&copy_statement);
                let statement = self.connection.prepare(&copy_statement)?;

//                dbg!(String::from_utf8_lossy(value_string.as_slice()));
                statement.copy_in(
                    &[],
                    &mut value_string.as_slice(),
                )?;

                // store in map and increase
                o.insert(id);
                self.next_dataset_id += 1;

                id
            }
        };

        // insert all units
        {
            let mut columns: Vec<String> = vec![self.database_settings.dataset_id_column.clone()];
            columns.extend_from_slice(self.unit_numeric_fields_hash.as_slice());
            columns.extend_from_slice(self.unit_textual_fields_hash.as_slice());

            let dataset_id_string = dataset_id.to_string();

            let mut values = WriterBuilder::new()
                .terminator(csv::Terminator::Any(b'\n'))
                .delimiter(b'\t')
                .quote(b'"')
                .escape(b'"')
                .has_headers(false)
                .from_writer(vec![]);

            // append units one by one to tsv
            for (unit_numeric_data, unit_textual_data) in &abcd_data.units {
                values.write_field(&dataset_id_string)?; // put id first

                for numeric_field in &self.unit_numeric_fields {
                    if let Some(&numeric_value) = unit_numeric_data.get(numeric_field) {
                        values.write_field(numeric_value.to_string())?;
                    } else {
                        values.write_field("")?;
                    }
                }

                for textual_field in &self.unit_textual_fields {
                    if let Some(textual_value) = unit_textual_data.get(textual_field) {
                        values.write_field(textual_value)?;
                    } else {
                        values.write_field("")?;
                    }
                }

                values.write_record(None::<&[u8]>)?; // terminate record
            }

            let copy_statement = format!(
                "COPY {}(\"{}\") FROM STDIN WITH ({})",
                self.database_settings.temp_unit_table, columns.join("\",\""), POSTGRES_CSV_CONFIGURATION
            );

            let statement = self.connection.prepare(&copy_statement)?;

//            dbg!(&value_string);
            statement.copy_in(
                &[],
                &mut values.into_inner()?.as_slice(),
            )?;
        }

        Ok(())
    }

    fn to_hash_value(&self, dataset_data: &(NumericMap, TextualMap)) -> String {
        let (dataset_numeric_data, dataset_textual_data) = dataset_data;

        let mut hash = String::new();

        for numeric_field in &self.dataset_numeric_fields {
            if let Some(&numeric_value) = dataset_numeric_data.get(numeric_field) {
                hash.push_str(&numeric_value.to_string());
            }
        }

        for textual_field in &self.dataset_textual_fields {
            if let Some(textual_value) = dataset_textual_data.get(textual_field) {
                hash.push_str(textual_value);
            }
        }

        hash
    }
}
