use std::collections::hash_map::Entry;
use std::collections::HashMap;

use csv::WriterBuilder;
use failure::{Error, Fail};
use log::debug;
use postgres::params::ConnectParams;
use postgres::params::Host;
use postgres::tls::openssl::OpenSsl;
use postgres::transaction::Transaction;
use postgres::{Connection, TlsMode};

use crate::abcd::{AbcdFields, AbcdResult, ValueMap};
use crate::settings;
use crate::storage::Field;

const POSTGRES_CSV_CONFIGURATION: &str =
    "DELIMITER '\t', NULL '', QUOTE '\"', ESCAPE '\"', FORMAT CSV";

/// A PostgreSQL storage DAO for storing datasets.
pub struct DatabaseSink<'s> {
    connection: Connection,
    database_settings: &'s settings::DatabaseSettings,
    dataset_fields: Vec<Field>,
    datasets_to_ids: HashMap<String, u32>,
    next_dataset_id: u32,
    unit_fields: Vec<Field>,
}

impl<'s> DatabaseSink<'s> {
    /// Create a new PostgreSQL storage sink (DAO).
    pub fn new(
        database_settings: &'s settings::DatabaseSettings,
        abcd_fields: &AbcdFields,
    ) -> Result<Self, Error> {
        // create storage connection params from the settings, including optional tls
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

        // fill lists of dataset and unit fields and give them a fixed order for the storage inserts
        let mut dataset_fields = Vec::new();
        let mut unit_fields = Vec::new();
        for field in abcd_fields {
            if field.global_field {
                dataset_fields.push(field.name.as_str().into());
            } else {
                unit_fields.push(field.name.as_str().into());
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
            datasets_to_ids: HashMap::new(),
            next_dataset_id: 1,
            unit_fields,
        };

        sink.initialize_temporary_schema(abcd_fields)?;

        Ok(sink)
    }

    /// Initialize the temporary storage schema.
    fn initialize_temporary_schema(&mut self, abcd_fields: &AbcdFields) -> Result<(), Error> {
        self.drop_temporary_tables()?;

        self.create_temporary_dataset_table(abcd_fields)?;

        self.create_temporary_unit_table(abcd_fields)?;

        self.create_and_fill_temporary_mapping_table()?;

        Ok(())
    }

    /// Create and fill a temporary mapping table from hashes to field names.
    fn create_and_fill_temporary_mapping_table(&mut self) -> Result<(), Error> {
        // create table
        self.connection.execute(
            &format!(
            "create table {schema}.{table}_translation (name text not null, hash text not null);",
            schema = self.database_settings.schema,
            table = self.database_settings.temp_dataset_table
        ),
            &[],
        )?;

        // fill table
        let statement = self.connection.prepare(&format!(
            "insert into {schema}.{table}_translation(name, hash) VALUES ($1, $2);",
            schema = self.database_settings.schema,
            table = self.database_settings.temp_dataset_table
        ))?;
        for field in self.dataset_fields.iter().chain(&self.unit_fields) {
            statement.execute(&[&field.name, &field.hash])?;
        }

        Ok(())
    }

    /// Create the temporary unit table
    fn create_temporary_unit_table(&mut self, abcd_fields: &AbcdFields) -> Result<(), Error> {
        let mut fields = vec![format!(
            "{} int not null",
            self.database_settings.dataset_id_column
        )];

        for field in &self.unit_fields {
            let abcd_field = abcd_fields
                .value_of(field.name.as_bytes())
                .ok_or_else(|| DatabaseSinkError::InconsistentUnitColumns(field.name.clone()))?;

            let data_type_string = if abcd_field.numeric {
                "double precision"
            } else {
                "text"
            };

            // TODO: enforce/filter not null
            // let null_string = if abcd_field.vat_mandatory { "NOT NULL" } else { "" }
            let null_string = "";

            fields.push(format!(
                "\"{hash}\" {datatype} {nullable}",
                hash = field.hash,
                datatype = data_type_string,
                nullable = null_string,
            ));
        }

        self.connection.execute(
            &format!(
                "CREATE TABLE {schema}.{table} ( {fields} );",
                schema = &self.database_settings.schema,
                table = self.database_settings.temp_unit_table,
                fields = fields.join(",")
            ),
            &[],
        )?;

        Ok(())
    }

    /// Create the temporary dataset table
    fn create_temporary_dataset_table(&mut self, abcd_fields: &AbcdFields) -> Result<(), Error> {
        let mut fields = vec![
            format!(
                "{} int primary key",
                self.database_settings.dataset_id_column
            ), // id
            format!(
                "{} text not null",
                self.database_settings.dataset_path_column
            ), // path
            format!(
                "{} text not null",
                self.database_settings.dataset_landing_page_column
            ), // landing page
            format!(
                "{} text not null",
                self.database_settings.dataset_provider_column
            ), // provider name
        ];

        for field in &self.dataset_fields {
            let abcd_field = abcd_fields
                .value_of(field.name.as_bytes())
                .ok_or_else(|| DatabaseSinkError::InconsistentDatasetColumns(field.name.clone()))?;

            let data_type_string = if abcd_field.numeric {
                "double precision"
            } else {
                "text"
            };

            // TODO: enforce/filter not null
            // let null_string = if abcd_field.vat_mandatory { "NOT NULL" } else { "" }
            let null_string = "";

            fields.push(format!(
                "\"{hash}\" {datatype} {nullable}",
                hash = field.hash,
                datatype = data_type_string,
                nullable = null_string,
            ));
        }

        self.connection.execute(
            &format!(
                "CREATE TABLE {schema}.{table} ( {fields} );",
                schema = &self.database_settings.schema,
                table = self.database_settings.temp_dataset_table,
                fields = fields.join(",")
            ),
            &[],
        )?;

        Ok(())
    }

    /// Drop all temporary tables if they exist.
    fn drop_temporary_tables(&mut self) -> Result<(), Error> {
        for statement in &[
            // unit temp table
            format!(
                "DROP TABLE IF EXISTS {schema}.{table};",
                schema = &self.database_settings.schema,
                table = &self.database_settings.temp_unit_table
            ),
            // dataset temp table
            format!(
                "DROP TABLE IF EXISTS {schema}.{table};",
                schema = &self.database_settings.schema,
                table = &self.database_settings.temp_dataset_table
            ),
            // translation temp table
            format!(
                "DROP TABLE IF EXISTS {schema}.{table}_translation;",
                schema = &self.database_settings.schema,
                table = &self.database_settings.temp_dataset_table
            ),
        ] {
            self.connection.execute(statement, &[])?;
        }

        Ok(())
    }

    /// Migrate the temporary tables to the persistent tables.
    /// Drops the old tables.
    pub fn migrate_schema(&mut self) -> Result<(), Error> {
        self.create_indexes_and_statistics()?;

        let transaction = self.connection.transaction_with(
            postgres::transaction::Config::new()
                .isolation_level(postgres::transaction::IsolationLevel::Serializable)
                .read_only(false),
        )?;

        self.drop_old_tables(&transaction)?;

        self.rename_temporary_tables(&transaction)?;

        self.rename_constraints_and_indexes(&transaction)?;

        self.create_listing_view(&transaction)?;

        transaction.commit()?;

        Ok(())
    }

    /// Drop old persistent tables.
    fn drop_old_tables(&self, transaction: &Transaction) -> Result<(), Error> {
        for statement in &[
            // listing view
            format!(
                "DROP VIEW IF EXISTS {schema}.{view_name};",
                schema = self.database_settings.schema,
                view_name = self.database_settings.listing_view
            ),
            // unit table
            format!(
                "DROP TABLE IF EXISTS {schema}.{table};",
                schema = self.database_settings.schema,
                table = self.database_settings.unit_table
            ),
            // dataset table
            format!(
                "DROP TABLE IF EXISTS {schema}.{table};",
                schema = self.database_settings.schema,
                table = self.database_settings.dataset_table
            ),
            // translation table
            format!(
                "DROP TABLE IF EXISTS {schema}.{table}_translation;",
                schema = self.database_settings.schema,
                table = self.database_settings.dataset_table
            ),
        ] {
            transaction.execute(statement, &[])?;
        }

        Ok(())
    }

    /// Rename temporary tables to persistent tables.
    fn rename_temporary_tables(&self, transaction: &Transaction) -> Result<(), Error> {
        for statement in &[
            // unit table
            format!(
                "ALTER TABLE {schema}.{temp_table} RENAME TO {table};",
                schema = self.database_settings.schema,
                temp_table = self.database_settings.temp_unit_table,
                table = self.database_settings.unit_table
            ),
            // dataset table
            format!(
                "ALTER TABLE {schema}.{temp_table} RENAME TO {table};",
                schema = self.database_settings.schema,
                temp_table = self.database_settings.temp_dataset_table,
                table = self.database_settings.dataset_table
            ),
            // translation table
            format!(
                "ALTER TABLE {schema}.{temp_table}_translation RENAME TO {table}_translation;",
                schema = self.database_settings.schema,
                temp_table = self.database_settings.temp_dataset_table,
                table = self.database_settings.dataset_table
            ),
        ] {
            transaction.execute(statement, &[])?;
        }

        Ok(())
    }

    /// Rename constraints and indexes from temporary to persistent.
    fn rename_constraints_and_indexes(&self, transaction: &Transaction) -> Result<(), Error> {
        for statement in &[
            // foreign key
            format!(
                "ALTER TABLE {schema}.{table} \
                 RENAME CONSTRAINT {temp_prefix}_{temp_suffix}_fk TO {prefix}_{suffix}_fk;",
                schema = &self.database_settings.schema,
                table = &self.database_settings.unit_table,
                temp_prefix = &self.database_settings.temp_unit_table,
                temp_suffix = &self.database_settings.dataset_id_column,
                prefix = &self.database_settings.unit_table,
                suffix = &self.database_settings.dataset_id_column
            ),
            // index
            format!(
                "ALTER INDEX {schema}.{temp_index}_idx RENAME TO {index}_idx;",
                schema = &self.database_settings.schema,
                temp_index = &self.database_settings.temp_unit_table,
                index = &self.database_settings.unit_table
            ),
        ] {
            transaction.execute(statement, &[])?;
        }

        Ok(())
    }

    /// Create foreign key relationships, indexes, clustering and statistics on the temporary tables.
    fn create_indexes_and_statistics(&mut self) -> Result<(), Error> {
        let foreign_key_statement = format!(
            "ALTER TABLE {schema}.{unit_table} \
             ADD CONSTRAINT {unit_table}_{dataset_id}_fk \
             FOREIGN KEY ({dataset_id}) REFERENCES {schema}.{dataset_table}({dataset_id});",
            schema = &self.database_settings.schema,
            unit_table = &self.database_settings.temp_unit_table,
            dataset_id = &self.database_settings.dataset_id_column,
            dataset_table = &self.database_settings.temp_dataset_table
        );
        debug!("{}", &foreign_key_statement);
        self.connection.execute(&foreign_key_statement, &[])?;
        let mut hasher = sha1::Sha1::new();
        let indexed_unit_column_names = self
            .database_settings
            .unit_indexed_columns
            .iter()
            .map(|field| {
                hasher.reset();
                hasher.update(field.as_bytes());
                hasher.digest().to_string()
            })
            .collect::<Vec<String>>();
        let unit_index_statement = format!(
            "CREATE INDEX {unit_table}_idx ON {schema}.{unit_table} \
             USING btree ({dataset_id}, \"{other}\");",
            schema = &self.database_settings.schema,
            unit_table = &self.database_settings.temp_unit_table,
            dataset_id = &self.database_settings.dataset_id_column,
            other = indexed_unit_column_names.join("\", \"")
        );
        debug!("{}", &unit_index_statement);
        self.connection.execute(&unit_index_statement, &[])?;
        let cluster_statement = format!(
            "CLUSTER {unit_table}_idx ON {schema}.{unit_table};",
            schema = &self.database_settings.schema,
            unit_table = &self.database_settings.temp_unit_table
        );
        debug!("{}", &cluster_statement);
        self.connection.execute(&cluster_statement, &[])?;
        let datasets_analyze_statement = format!(
            "VACUUM ANALYZE {schema}.{dataset_table};",
            schema = &self.database_settings.schema,
            dataset_table = &self.database_settings.temp_dataset_table
        );
        debug!("{}", &datasets_analyze_statement);
        self.connection.execute(&datasets_analyze_statement, &[])?;
        let units_analyze_statement = format!(
            "VACUUM ANALYZE {schema}.{unit_table};",
            schema = &self.database_settings.schema,
            unit_table = &self.database_settings.temp_unit_table
        );
        debug!("{}", &units_analyze_statement);
        self.connection.execute(&units_analyze_statement, &[])?;

        Ok(())
    }

    /// Create view that provides a listing view
    pub fn create_listing_view(&self, transaction: &Transaction) -> Result<(), Error> {
        // TODO: replace full names with settings call
        let mut hasher = sha1::Sha1::new();

        hasher.update(b"/DataSets/DataSet/Metadata/Description/Representation/Title");
        let dataset_name = hasher.digest().to_string();
        hasher.reset();

        hasher.update(b"/DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/CoordinatesLatLong/LatitudeDecimal");
        let latitude_column_hash = hasher.digest().to_string();
        hasher.reset();

        hasher.update(b"/DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/CoordinatesLatLong/LongitudeDecimal");
        let longitude_column_hash = hasher.digest().to_string();
        hasher.reset();

        let view_statement = format!(
            r#"
            CREATE VIEW {schema}.{view_name} AS (
            select link, dataset, file, provider, isGeoReferenced as available, isGeoReferenced
            from (
                   select {dataset_landing_page_column} as link,
                          "{dataset_name}"              as dataset,
                          {dataset_path_column}         as file,
                          {dataset_provider_column}     as provider,
                          (SELECT EXISTS(
                              select * from {schema}.{unit_table}
                              where {dataset_table}.{dataset_id_column} = {unit_table}.{dataset_id_column}
                                and "{latitude_column_hash}" is not null
                                and "{longitude_column_hash}" is not null
                            ))                 as isGeoReferenced
                   from {schema}.{dataset_table}
            ) sub);"#,
            schema = self.database_settings.schema,
            view_name = self.database_settings.listing_view,
            dataset_name = dataset_name,
            dataset_landing_page_column = self.database_settings.dataset_landing_page_column,
            dataset_path_column = self.database_settings.dataset_path_column,
            dataset_provider_column = self.database_settings.dataset_provider_column,
            dataset_table = self.database_settings.dataset_table,
            unit_table = self.database_settings.unit_table,
            dataset_id_column = self.database_settings.dataset_id_column,
            latitude_column_hash = latitude_column_hash,
            longitude_column_hash = longitude_column_hash,
        );

        transaction.execute(&view_statement, &[])?;

        Ok(())
    }

    /// Insert a dataset and its units into the temporary tables.
    pub fn insert_dataset(&mut self, abcd_data: &AbcdResult) -> Result<(), Error> {
        // retrieve the id for the dataset
        // if the dataset is not found, it is necessary to create a dataset storage entry at first
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
    fn insert_dataset_metadata(
        database_settings: &settings::DatabaseSettings,
        connection: &Connection,
        dataset_fields: &[Field],
        abcd_data: &AbcdResult,
        id: u32,
    ) -> Result<(), Error> {
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
        values.write_field(abcd_data.provider_name.clone())?;
        for field in dataset_fields {
            columns.push(&field.hash);
            if let Some(value) = abcd_data.dataset.get(&field.name) {
                values.write_field(value.to_string())?;
            } else {
                values.write_field("")?;
            }
        }
        // terminate record
        values.write_record(None::<&[u8]>)?;

        let copy_statement = format!(
            "COPY {schema}.{table}(\"{columns}\") FROM STDIN WITH ({options})",
            schema = database_settings.schema,
            table = database_settings.temp_dataset_table,
            columns = columns.join("\",\""),
            options = POSTGRES_CSV_CONFIGURATION
        );
        // dbg!(&copy_statement);

        let value_string = values.into_inner()?;
        // dbg!(String::from_utf8_lossy(value_string.as_slice()));

        let statement = connection.prepare(&copy_statement)?;
        statement.copy_in(&[], &mut value_string.as_slice())?;

        Ok(())
    }

    /// Insert the dataset units into the temporary schema
    fn insert_units(&mut self, abcd_data: &AbcdResult, dataset_id: u32) -> Result<(), Error> {
        let mut columns: Vec<String> = vec![self.database_settings.dataset_id_column.clone()];
        columns.extend(self.unit_fields.iter().map(|field| field.name.clone()));

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
                if let Some(value) = unit_data.get(&field.name) {
                    values.write_field(value.to_string())?;
                } else {
                    values.write_field("")?;
                }
            }

            values.write_record(None::<&[u8]>)?; // terminate record
        }

        let copy_statement = format!(
            "COPY {schema}.{table}(\"{columns}\") FROM STDIN WITH ({options})",
            schema = self.database_settings.schema,
            table = self.database_settings.temp_unit_table,
            columns = columns.join("\",\""),
            options = POSTGRES_CSV_CONFIGURATION
        );

        let statement = self.connection.prepare(&copy_statement)?;
        //            dbg!(&value_string);
        statement.copy_in(&[], &mut values.into_inner()?.as_slice())?;

        Ok(())
    }

    /// Combines all values of the dataset's metadata into a new string.
    fn to_combined_string(&self, dataset_data: &ValueMap) -> String {
        let mut hash = String::new();

        for field in &self.dataset_fields {
            if let Some(value) = dataset_data.get(&field.name) {
                hash.push_str(&value.to_string());
            }
        }

        hash
    }
}

/// An error enum for different storage sink errors.
#[derive(Debug, Fail)]
pub enum DatabaseSinkError {
    /// This error occurs when there is an inconsistency between the ABCD dataset data and the sink's columns.
    #[fail(display = "Inconsistent dataset columns: {}", 0)]
    InconsistentDatasetColumns(String),
    /// This error occurs when there is an inconsistency between the ABCD unit data and the sink's columns.
    #[fail(display = "Inconsistent unit columns: {}", 0)]
    InconsistentUnitColumns(String),
}
