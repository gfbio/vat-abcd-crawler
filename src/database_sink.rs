use crate::abcd_fields::AbcdField;
use crate::abcd_parser::AbcdResult;
use crate::abcd_parser::ValueMap;
use crate::settings;
use failure::{Error, Fail};
use log::debug;
use postgres::params::ConnectParams;
use postgres::params::Host;
use postgres::tls::openssl::OpenSsl;
use postgres::{Connection, TlsMode};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use csv::WriterBuilder;
use postgres::transaction::Transaction;

const POSTGRES_CSV_CONFIGURATION: &str = "DELIMITER '\t', NULL '', QUOTE '\"', ESCAPE '\"', FORMAT CSV";

/// A PostgreSQL database DAO for storing datasets.
pub struct DatabaseSink<'s> {
    connection: Connection,
    database_settings: &'s settings::Database,
    dataset_fields: Vec<String>,
    dataset_fields_hash: Vec<String>,
    datasets_to_ids: HashMap<String, u32>,
    next_dataset_id: u32,
    unit_fields: Vec<String>,
    unit_fields_hash: Vec<String>,
}

impl<'s> DatabaseSink<'s> {
    /// Create a new PostgreSQL database sink (DAO).
    pub fn new(database_settings: &'s settings::Database,
               abcd_fields: &HashMap<Vec<u8>, AbcdField>) -> Result<Self, Error> {
        // create database connection params from the settings, including optional tls
        let negotiator = if database_settings.tls {
            Some(OpenSsl::new()?)
        } else {
            None
        };
        let connection_params = ConnectParams::builder()
            .user(&database_settings.user, Some(&database_settings.password))
            .port(database_settings.port)
            .database(&database_settings.database)
            .build(Host::Tcp(database_settings.host.clone()));

        // fill lists of dataset and unit fields and give them a fixed order for the database inserts
        let mut dataset_fields = Vec::new();
        let mut dataset_fields_hash = Vec::new();
        let mut unit_fields = Vec::new();
        let mut unit_fields_hash = Vec::new();
        let mut hasher = sha1::Sha1::new();
        for field in abcd_fields.values() {
            let hash = {
                hasher.reset();
                hasher.update(field.field.as_bytes());
                hasher.digest().to_string()
            };
            if field.global_field {
                dataset_fields.push(field.field.clone());
                dataset_fields_hash.push(hash);
            } else {
                unit_fields.push(field.field.clone());
                unit_fields_hash.push(hash);
            }
        }

        let mut sink = Self {
            connection: Connection::connect(
                connection_params,
                if let Some(negotiator) = &negotiator {
                    TlsMode::Prefer(negotiator)
                } else {
                    TlsMode::None
                },
            )?,
            database_settings,
            dataset_fields,
            dataset_fields_hash,
            datasets_to_ids: HashMap::new(),
            next_dataset_id: 1,
            unit_fields,
            unit_fields_hash,
        };

        sink.initialize_temporary_schema(abcd_fields)?;

        Ok(sink)
    }

    /// Initialize the temporary database schema.
    fn initialize_temporary_schema(&mut self, abcd_fields: &HashMap<Vec<u8>, AbcdField>) -> Result<(), Error> {
        self.drop_temporary_tables()?;

        self.create_temporary_dataset_table(abcd_fields)?;

        self.create_temporary_unit_table(abcd_fields)?;

        self.create_and_fill_temporary_mapping_table()?;

        Ok(())
    }

    /// Create and fill a temporary mapping table from hashes to field names.
    fn create_and_fill_temporary_mapping_table(&mut self) -> Result<(), Error> {
        // create table
        self.connection.execute(&format!(
            "create table {}_translation (name text not null, hash text not null);",
            self.database_settings.temp_dataset_table
        ), &[])?;

        // fill table
        let statement = self.connection.prepare(&format!(
            "insert into {}_translation(name, hash) VALUES ($1, $2);",
            self.database_settings.temp_dataset_table
        ))?;
        for (name, hash) in self.dataset_fields.iter().zip(&self.dataset_fields_hash) {
            statement.execute(&[name, hash])?;
        }
        for (name, hash) in self.unit_fields.iter().zip(&self.unit_fields_hash) {
            statement.execute(&[name, hash])?;
        }

        Ok(())
    }

    /// Create the temporary unit table
    fn create_temporary_unit_table(&mut self, abcd_fields: &HashMap<Vec<u8>, AbcdField>) -> Result<(), Error> {
        let mut fields = vec![
            format!("{} int not null", self.database_settings.dataset_id_column),
        ];

        for (field, hash) in self.unit_fields.iter().zip(&self.unit_fields_hash) {
            let abcd_field = abcd_fields.get(field.as_bytes())
                .ok_or_else(|| DatabaseSinkError::InconsistentUnitColumns(field.clone()))?;

            let data_type_string = if abcd_field.numeric { "double precision" } else { "text" };

            // TODO: enforce/filter not null
            // let null_string = if abcd_field.vat_mandatory { "NOT NULL" } else { "" }
            let null_string = "";

            fields.push(format!("\"{}\" {} {}", hash, data_type_string, null_string));
        }

        self.connection.execute(&format!(
            "create table {} ( {} );", self.database_settings.temp_unit_table, fields.join(",")
        ), &[])?;

        Ok(())
    }

    /// Create the temporary dataset table
    fn create_temporary_dataset_table(&mut self, abcd_fields: &HashMap<Vec<u8>, AbcdField>) -> Result<(), Error> {
        let mut fields = vec![
            format!("{} int primary key", self.database_settings.dataset_id_column), // id
            format!("{} text not null", self.database_settings.dataset_path_column), // path
            format!("{} text not null", self.database_settings.dataset_landing_page_column), // landing page
            format!("{} text not null", self.database_settings.dataset_provider_column), // provider name
        ];

        for (field, hash) in self.dataset_fields.iter().zip(&self.dataset_fields_hash) {
            let abcd_field = abcd_fields.get(field.as_bytes())
                .ok_or_else(|| DatabaseSinkError::InconsistentDatasetColumns(field.clone()))?;

            let data_type_string = if abcd_field.numeric { "double precision" } else { "text" };

            // TODO: enforce/filter not null
            // let null_string = if abcd_field.vat_mandatory { "NOT NULL" } else { "" }
            let null_string = "";

            fields.push(format!("\"{}\" {} {}", hash, data_type_string, null_string));
        }

        self.connection.execute(&format!(
            "create table {} ( {} );", self.database_settings.temp_dataset_table, fields.join(",")
        ), &[])?;

        Ok(())
    }

    /// Drop all temporary tables if they exist.
    fn drop_temporary_tables(&mut self) -> Result<(), Error> {
        self.connection.execute(&format!(
            "DROP TABLE IF EXISTS {};", &self.database_settings.temp_unit_table
        ), &[])?;
        self.connection.execute(&format!(
            "DROP TABLE IF EXISTS {};", &self.database_settings.temp_dataset_table
        ), &[])?;
        self.connection.execute(&format!(
            "DROP TABLE IF EXISTS {}_translation;", &self.database_settings.temp_dataset_table
        ), &[])?;

        Ok(())
    }

    /// Migrate the temporary tables to the persistent tables.
    /// Drops the old tables.
    pub fn migrate_schema(&mut self) -> Result<(), Error> {
        self.create_indexes_and_statistics()?;

        let transaction = self.connection.transaction_with(
            postgres::transaction::Config::new()
                .isolation_level(postgres::transaction::IsolationLevel::Serializable)
                .read_only(false)
        )?;

        self.drop_old_tables(&transaction)?;

        self.rename_temporary_tables(&transaction)?;

        self.rename_constraints_and_indexes(&transaction)?;

        transaction.commit()?;

        Ok(())
    }

    /// Drop old persistent tables.
    fn drop_old_tables(&self, transaction: &Transaction) -> Result<(), Error> {
        // unit table
        transaction.execute(&format!(
            "DROP TABLE IF EXISTS {};", &self.database_settings.unit_table
        ), &[])?;
        // dataset table
        transaction.execute(&format!(
            "DROP TABLE IF EXISTS {};", &self.database_settings.dataset_table
        ), &[])?;
        // translation table
        transaction.execute(&format!(
            "DROP TABLE IF EXISTS {}_translation;", &self.database_settings.dataset_table
        ), &[])?;

        Ok(())
    }

    /// Rename temporary tables to persistent tables.
    fn rename_temporary_tables(&self, transaction: &Transaction) -> Result<(), Error> {
        // unit table
        transaction.execute(&format!(
            "ALTER TABLE {} RENAME TO {};", &self.database_settings.temp_unit_table, &self.database_settings.unit_table
        ), &[])?;

        // dataset table
        transaction.execute(&format!(
            "ALTER TABLE {} RENAME TO {};", &self.database_settings.temp_dataset_table, &self.database_settings.dataset_table
        ), &[])?;

        // translation table
        transaction.execute(&format!(
            "ALTER TABLE {}_translation RENAME TO {}_translation;", &self.database_settings.temp_dataset_table, &self.database_settings.dataset_table
        ), &[])?;

        Ok(())
    }

    /// Rename constraints and indexes from temporary to persistent.
    fn rename_constraints_and_indexes(&self, transaction: &Transaction) -> Result<(), Error> {
        transaction.execute(&format!(
            "ALTER TABLE {} RENAME CONSTRAINT {}_{}_fk TO {}_{}_fk;",
            &self.database_settings.unit_table,
            &self.database_settings.temp_unit_table, &self.database_settings.dataset_id_column,
            &self.database_settings.unit_table, &self.database_settings.dataset_id_column
        ), &[])?;
        transaction.execute(&format!(
            "ALTER INDEX {}_idx RENAME TO {}_idx;",
            &self.database_settings.temp_unit_table, &self.database_settings.unit_table
        ), &[])?;

        Ok(())
    }

    /// Create foreign key relationships, indexes, clustering and statistics on the temporary tables.
    fn create_indexes_and_statistics(&mut self) -> Result<(), Error> {
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
        self.connection.execute(&foreign_key_statement, &[])?;
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
        self.connection.execute(&unit_index_statement, &[])?;
        let cluster_statement = format!(
            "CLUSTER {}_idx ON {};",
            &self.database_settings.temp_unit_table,
            &self.database_settings.temp_unit_table
        );
        debug!("{}", &cluster_statement);
        self.connection.execute(&cluster_statement, &[])?;
        let datasets_analyze_statement = format!(
            "VACUUM ANALYZE {};",
            &self.database_settings.temp_dataset_table
        );
        debug!("{}", &datasets_analyze_statement);
        self.connection.execute(&datasets_analyze_statement, &[])?;
        let units_analyze_statement = format!(
            "VACUUM ANALYZE {};",
            &self.database_settings.temp_unit_table
        );
        debug!("{}", &units_analyze_statement);
        self.connection.execute(&units_analyze_statement, &[])?;

        Ok(())
    }

    /// Insert a dataset and its units into the temporary tables.
    pub fn insert_dataset(&mut self, abcd_data: &AbcdResult) -> Result<(), Error> {
        // retrieve the id for the dataset
        // if the dataset is not found, it is necessary to create a dataset database entry at first
        let dataset_unique_string = self.to_combined_string(&abcd_data.dataset);
        let dataset_id = match self.datasets_to_ids.entry(dataset_unique_string) {
            Entry::Occupied(e) => *e.get(),
            Entry::Vacant(o) => {
                // retrieve next dataset id
                let id = self.next_dataset_id;

                Self::insert_dataset_metadata(
                    &self.database_settings,
                    &self.connection,
                    self.dataset_fields.as_slice(),
                    self.dataset_fields_hash.as_slice(),
                    abcd_data,
                    id,
                )?;

                // store id in map and increase next id variable
                o.insert(id);
                self.next_dataset_id += 1;

                id
            }
        };

        self.insert_units(&abcd_data, dataset_id)?;

        Ok(())
    }

    /// Insert the dataset metadata into the temporary schema
    fn insert_dataset_metadata(database_settings: &settings::Database,
                               connection: &Connection,
                               dataset_fields: &[String],
                               dataset_fields_hash: &[String],
                               abcd_data: &AbcdResult,
                               id: u32) -> Result<(), Error> {
        let mut values = WriterBuilder::new()
            .terminator(csv::Terminator::Any(b'\n'))
            .delimiter(b'\t')
            .quote(b'"')
            .escape(b'"')
            .has_headers(false)
            .from_writer(vec![]);
        let mut columns: Vec<&str> = vec![
            database_settings.dataset_id_column.as_ref(),
            database_settings.dataset_path_column.as_ref(),
            database_settings.dataset_landing_page_column.as_ref(),
            database_settings.dataset_provider_column.as_ref(),
        ];
        values.write_field(id.to_string())?;
        values.write_field(abcd_data.dataset_path.clone())?;
        values.write_field(abcd_data.landing_page.clone())?;
        values.write_field(abcd_data.provider_id.clone())?;
        for (field, hash) in dataset_fields.iter().zip(dataset_fields_hash.iter()) {
            columns.push(&hash);
            if let Some(value) = abcd_data.dataset.get(field) {
                values.write_field(value.to_string())?;
            } else {
                values.write_field("")?;
            }
        }
        // terminate record
        values.write_record(None::<&[u8]>)?;

        let copy_statement = format!(
            "COPY {}(\"{}\") FROM STDIN WITH ({})",
            database_settings.temp_dataset_table, columns.join("\",\""),
            POSTGRES_CSV_CONFIGURATION
        );
        // dbg!(&copy_statement);

        let value_string = values.into_inner()?;
        // dbg!(String::from_utf8_lossy(value_string.as_slice()));

        let statement = connection.prepare(&copy_statement)?;
        statement.copy_in(
            &[],
            &mut value_string.as_slice(),
        )?;

        Ok(())
    }

    /// Insert the dataset units into the temporary schema
    fn insert_units(&mut self, abcd_data: &AbcdResult, dataset_id: u32) -> Result<(), Error> {
        let mut columns: Vec<String> = vec![self.database_settings.dataset_id_column.clone()];
        columns.extend_from_slice(self.unit_fields_hash.as_slice());

        let dataset_id_string = dataset_id.to_string();

        let mut values = WriterBuilder::new()
            .terminator(csv::Terminator::Any(b'\n'))
            .delimiter(b'\t')
            .quote(b'"')
            .escape(b'"')
            .has_headers(false)
            .from_writer(vec![]);

        // append units one by one to tsv
        for unit_data in &abcd_data.units {
            values.write_field(&dataset_id_string)?; // put id first

            for field in &self.unit_fields {
                if let Some(value) = unit_data.get(field) {
                    values.write_field(value.to_string())?;
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

        Ok(())
    }

    /// Combines all values of the dataset's metadata into a new string.
    fn to_combined_string(&self, dataset_data: &ValueMap) -> String {
        let mut hash = String::new();

        for field in &self.dataset_fields {
            if let Some(value) = dataset_data.get(field) {
                hash.push_str(&value.to_string());
            }
        }

        hash
    }
}

/// An error enum for different database sink errors.
#[derive(Debug, Fail)]
pub enum DatabaseSinkError {
    /// This error occurs when there is an inconsistency between the ABCD dataset data and the sink's columns.
    #[fail(display = "Inconsistent dataset columns: {}", 0)]
    InconsistentDatasetColumns(String),

    /// This error occurs when there is an inconsistency between the ABCD unit data and the sink's columns.
    #[fail(display = "Inconsistent unit columns: {}", 0)]
    InconsistentUnitColumns(String),
}
