use std::io::Write;

use csv::WriterBuilder;
use failure::{Error, Fail};
use log::debug;
use openssl::ssl::{SslConnector, SslMethod};
use postgres::{config::SslMode, Client, Config, IsolationLevel, NoTls, Transaction};
use postgres_openssl::MakeTlsConnector;

use crate::abcd::{AbcdFields, AbcdResult};
use crate::settings;
use crate::settings::DatabaseSettings;
use crate::storage::{Field, SurrogateKey, SurrogateKeyType};

const POSTGRES_CSV_CONFIGURATION: &str =
    "DELIMITER '\t', NULL '', QUOTE '\"', ESCAPE '\"', FORMAT CSV";

/// A PostgreSQL storage DAO for storing datasets.
pub struct DatabaseSink<'s> {
    connection: Client,
    database_settings: &'s settings::DatabaseSettings,
    dataset_fields: Vec<Field>,
    surrogate_key: SurrogateKey,
    unit_fields: Vec<Field>,
}

impl<'s> DatabaseSink<'s> {
    /// Create a new PostgreSQL storage sink (DAO).
    pub fn new(
        database_settings: &'s settings::DatabaseSettings,
        abcd_fields: &AbcdFields,
    ) -> Result<Self, Error> {
        let connection = <DatabaseSink<'s>>::create_database_connection(database_settings)?;

        let (dataset_fields, unit_fields) =
            <DatabaseSink<'s>>::create_lists_of_dataset_and_unit_fields(abcd_fields);

        let mut sink = Self {
            connection,
            database_settings,
            dataset_fields,
            surrogate_key: Default::default(),
            unit_fields,
        };

        sink.initialize_temporary_schema(abcd_fields)?;

        Ok(sink)
    }

    fn create_database_connection(database_settings: &DatabaseSettings) -> Result<Client, Error> {
        let mut connection_params = Config::new();
        connection_params
            .user(&database_settings.user)
            .password(&database_settings.password)
            .port(database_settings.port)
            .dbname(&database_settings.database)
            .host(&database_settings.host)
            .ssl_mode(SslMode::Prefer);

        let connection = if database_settings.tls {
            let mut builder = SslConnector::builder(SslMethod::tls())?;
            builder.set_ca_file("database_cert.pem")?;
            let connector = MakeTlsConnector::new(builder.build());
            connection_params.connect(connector)?
        } else {
            connection_params.connect(NoTls)?
        };

        Ok(connection)
    }

    fn create_lists_of_dataset_and_unit_fields(
        abcd_fields: &AbcdFields,
    ) -> (Vec<Field>, Vec<Field>) {
        let mut dataset_fields = Vec::new();
        let mut unit_fields = Vec::new();

        for field in abcd_fields {
            if field.global_field {
                dataset_fields.push(field.name.as_str().into());
            } else {
                unit_fields.push(field.name.as_str().into());
            }
        }

        (dataset_fields, unit_fields)
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
        let statement = self.connection.prepare(&format!(
            "create table {schema}.{table}_translation (name text not null, hash text not null);",
            schema = self.database_settings.schema,
            table = self.database_settings.temp_dataset_table
        ))?;
        self.connection.execute(&statement, &[])?;

        // fill table
        let statement = self.connection.prepare(&format!(
            "insert into {schema}.{table}_translation(name, hash) VALUES ($1, $2);",
            schema = self.database_settings.schema,
            table = self.database_settings.temp_dataset_table
        ))?;
        for field in self.dataset_fields.iter().chain(&self.unit_fields) {
            self.connection
                .execute(&statement, &[&field.name, &field.hash])?;
        }

        Ok(())
    }

    /// Create the temporary unit table
    fn create_temporary_unit_table(&mut self, abcd_fields: &AbcdFields) -> Result<(), Error> {
        let mut fields = vec![
            format!(
                "{} int not null",
                self.database_settings.surrogate_key_column,
            ),
            "geom geometry(Point)".to_owned(),
        ];

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

        let statement = self.connection.prepare(&format!(
            "CREATE TABLE {schema}.{table} ( {fields} );",
            schema = &self.database_settings.schema,
            table = self.database_settings.temp_unit_table,
            fields = fields.join(",")
        ))?;
        self.connection.execute(&statement, &[])?;

        Ok(())
    }

    /// Create the temporary dataset table
    fn create_temporary_dataset_table(&mut self, abcd_fields: &AbcdFields) -> Result<(), Error> {
        let mut fields = vec![
            format!(
                "{} int primary key",
                self.database_settings.surrogate_key_column,
            ), // surrogate key
            format!("{} text not null", self.database_settings.dataset_id_column), // id
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

        let statement = self.connection.prepare(&format!(
            "CREATE TABLE {schema}.{table} ( {fields} );",
            schema = &self.database_settings.schema,
            table = self.database_settings.temp_dataset_table,
            fields = fields.join(",")
        ))?;
        self.connection.execute(&statement, &[])?;

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
            let statement = self.connection.prepare(statement)?;
            self.connection.execute(&statement, &[])?;
        }

        Ok(())
    }

    /// Migrate the temporary tables to the persistent tables.
    /// Drops the old tables.
    pub fn migrate_schema(&mut self) -> Result<(), Error> {
        self.create_indexes_and_statistics()?;

        let mut transaction = self
            .connection
            .build_transaction()
            .isolation_level(IsolationLevel::Serializable)
            .read_only(false)
            .start()?;

        Self::drop_old_tables(self.database_settings, &mut transaction)?;

        Self::rename_temporary_tables(self.database_settings, &mut transaction)?;

        Self::rename_constraints_and_indexes(self.database_settings, &mut transaction)?;

        Self::create_listing_view(
            self.database_settings,
            &self.dataset_fields,
            &self.unit_fields,
            &mut transaction,
        )?;

        transaction.commit()?;

        Ok(())
    }

    /// Drop old persistent tables.
    fn drop_old_tables(
        database_settings: &settings::DatabaseSettings,
        transaction: &mut Transaction,
    ) -> Result<(), Error> {
        for statement in &[
            // listing view
            format!(
                "DROP VIEW IF EXISTS {schema}.{view_name};",
                schema = database_settings.schema,
                view_name = database_settings.listing_view
            ),
            // unit table
            format!(
                "DROP TABLE IF EXISTS {schema}.{table};",
                schema = database_settings.schema,
                table = database_settings.unit_table
            ),
            // dataset table
            format!(
                "DROP TABLE IF EXISTS {schema}.{table};",
                schema = database_settings.schema,
                table = database_settings.dataset_table
            ),
            // translation table
            format!(
                "DROP TABLE IF EXISTS {schema}.{table}_translation;",
                schema = database_settings.schema,
                table = database_settings.dataset_table
            ),
        ] {
            let statement = transaction.prepare(statement)?;
            transaction.execute(&statement, &[])?;
        }

        Ok(())
    }

    /// Rename temporary tables to persistent tables.
    fn rename_temporary_tables(
        database_settings: &settings::DatabaseSettings,
        transaction: &mut Transaction,
    ) -> Result<(), Error> {
        for statement in &[
            // unit table
            format!(
                "ALTER TABLE {schema}.{temp_table} RENAME TO {table};",
                schema = database_settings.schema,
                temp_table = database_settings.temp_unit_table,
                table = database_settings.unit_table
            ),
            // dataset table
            format!(
                "ALTER TABLE {schema}.{temp_table} RENAME TO {table};",
                schema = database_settings.schema,
                temp_table = database_settings.temp_dataset_table,
                table = database_settings.dataset_table
            ),
            // translation table
            format!(
                "ALTER TABLE {schema}.{temp_table}_translation RENAME TO {table}_translation;",
                schema = database_settings.schema,
                temp_table = database_settings.temp_dataset_table,
                table = database_settings.dataset_table
            ),
        ] {
            let statement = transaction.prepare(statement)?;
            transaction.execute(&statement, &[])?;
        }

        Ok(())
    }

    /// Rename constraints and indexes from temporary to persistent.
    fn rename_constraints_and_indexes(
        database_settings: &settings::DatabaseSettings,
        transaction: &mut Transaction,
    ) -> Result<(), Error> {
        for statement in &[
            // primary key
            // foreign key
            format!(
                "ALTER TABLE {schema}.{table} \
                 RENAME CONSTRAINT {temp_table}_pkey TO {table}_pkey;",
                schema = &database_settings.schema,
                table = &database_settings.dataset_table,
                temp_table = &database_settings.temp_dataset_table,
            ),
            // foreign key
            format!(
                "ALTER TABLE {schema}.{table} \
                 RENAME CONSTRAINT {temp_prefix}_{temp_suffix}_fk TO {prefix}_{suffix}_fk;",
                schema = &database_settings.schema,
                table = &database_settings.unit_table,
                temp_prefix = &database_settings.temp_unit_table,
                temp_suffix = &database_settings.surrogate_key_column,
                prefix = &database_settings.unit_table,
                suffix = &database_settings.surrogate_key_column
            ),
            // index
            format!(
                "ALTER INDEX {schema}.{temp_index}_idx RENAME TO {index}_idx;",
                schema = &database_settings.schema,
                temp_index = &database_settings.temp_unit_table,
                index = &database_settings.unit_table
            ),
            // geom index
            format!(
                "ALTER INDEX {schema}.{temp_index}_geom_idx RENAME TO {index}_geom_idx;",
                schema = &database_settings.schema,
                temp_index = &database_settings.temp_unit_table,
                index = &database_settings.unit_table
            ),
        ] {
            let statement = transaction.prepare(statement)?;
            transaction.execute(&statement, &[])?;
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
            dataset_id = &self.database_settings.surrogate_key_column,
            dataset_table = &self.database_settings.temp_dataset_table
        );
        debug!("{}", &foreign_key_statement);
        let foreign_key_statement = self.connection.prepare(&foreign_key_statement)?;
        self.connection.execute(&foreign_key_statement, &[])?;

        let indexed_unit_column_names = self
            .database_settings
            .unit_indexed_columns
            .iter()
            .map(Field::from)
            .map(|field| field.hash)
            .collect::<Vec<String>>();
        let unit_index_statement = format!(
            "CREATE INDEX {unit_table}_idx ON {schema}.{unit_table} \
             USING btree ({surrogate_key_column} {other_begin}{other}{other_end});",
            schema = &self.database_settings.schema,
            unit_table = &self.database_settings.temp_unit_table,
            surrogate_key_column = &self.database_settings.surrogate_key_column,
            other_begin = if indexed_unit_column_names.is_empty() {
                ""
            } else {
                ", \""
            },
            other = indexed_unit_column_names.join("\", \""),
            other_end = if indexed_unit_column_names.is_empty() {
                ""
            } else {
                "\""
            },
        );
        debug!("{}", &unit_index_statement);
        let unit_index_statement = self.connection.prepare(&unit_index_statement)?;
        self.connection.execute(&unit_index_statement, &[])?;

        let geom_index_statement = format!(
            "CREATE INDEX {unit_table}_geom_idx ON {schema}.{unit_table} \
             USING SPGIST (geom);",
            schema = &self.database_settings.schema,
            unit_table = &self.database_settings.temp_unit_table,
        );
        debug!("{}", &geom_index_statement);
        let geom_index_statement = self.connection.prepare(&geom_index_statement)?;
        self.connection.execute(&geom_index_statement, &[])?;

        let cluster_statement = format!(
            "CLUSTER {unit_table}_idx ON {schema}.{unit_table};",
            schema = &self.database_settings.schema,
            unit_table = &self.database_settings.temp_unit_table
        );
        debug!("{}", &cluster_statement);
        let cluster_statement = self.connection.prepare(&cluster_statement)?;
        self.connection.execute(&cluster_statement, &[])?;

        let datasets_analyze_statement = format!(
            "VACUUM ANALYZE {schema}.{dataset_table};",
            schema = &self.database_settings.schema,
            dataset_table = &self.database_settings.temp_dataset_table
        );
        debug!("{}", &datasets_analyze_statement);
        let datasets_analyze_statement = self.connection.prepare(&datasets_analyze_statement)?;
        self.connection.execute(&datasets_analyze_statement, &[])?;

        let units_analyze_statement = format!(
            "VACUUM ANALYZE {schema}.{unit_table};",
            schema = &self.database_settings.schema,
            unit_table = &self.database_settings.temp_unit_table
        );
        debug!("{}", &units_analyze_statement);
        let units_analyze_statement = self.connection.prepare(&units_analyze_statement)?;
        self.connection.execute(&units_analyze_statement, &[])?;

        Ok(())
    }

    /// Create view that provides a listing view
    pub fn create_listing_view(
        database_settings: &settings::DatabaseSettings,
        dataset_fields: &[Field],
        unit_fields: &[Field],
        transaction: &mut Transaction,
    ) -> Result<(), Error> {
        // TODO: replace full names with settings call

        let dataset_title = if let Some(field) = dataset_fields.iter().find(|field| {
            field.name == "/DataSets/DataSet/Metadata/Description/Representation/Title"
        }) {
            format!("\"{}\"", field.hash)
        } else {
            "''".to_string()
        };

        let latitude_column = if let Some(field) = unit_fields.iter().find(|field| {
            field.name == "/DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/CoordinatesLatLong/LatitudeDecimal"
        }) {
            format!("\"{}\"", field.hash)
        } else {
            "NULL".to_string()
        };

        let longitude_column = if let Some(field) = unit_fields.iter().find(|field| {
            field.name == "/DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/CoordinatesLatLong/LongitudeDecimal"
        }) {
            format!("\"{}\"", field.hash)
        } else {
            "NULL".to_string()
        };

        let view_statement = format!(
            r#"
            CREATE VIEW {schema}.{view_name} AS (
            select link, dataset, id, provider, isGeoReferenced as available, isGeoReferenced
            from (
                   select {dataset_landing_page_column} as link,
                          {dataset_title}               as dataset,
                          {dataset_id_column}           as id,
                          {dataset_provider_column}     as provider,
                          (SELECT EXISTS(
                              select * from {schema}.{unit_table}
                              where {dataset_table}.{surrogate_key_column} = {unit_table}.{surrogate_key_column}
                                and {latitude_column} is not null
                                and {longitude_column} is not null
                            ))                 as isGeoReferenced
                   from {schema}.{dataset_table}
            ) sub);"#,
            schema = database_settings.schema,
            view_name = database_settings.listing_view,
            dataset_title = dataset_title,
            dataset_landing_page_column = database_settings.dataset_landing_page_column,
            dataset_id_column = database_settings.dataset_id_column,
            dataset_provider_column = database_settings.dataset_provider_column,
            dataset_table = database_settings.dataset_table,
            unit_table = database_settings.unit_table,
            surrogate_key_column = database_settings.surrogate_key_column,
            latitude_column = latitude_column,
            longitude_column = longitude_column,
        );

        let view_statement = transaction.prepare(&view_statement)?;
        transaction.execute(&view_statement, &[])?;

        Ok(())
    }

    /// Insert a dataset and its units into the temporary tables.
    pub fn insert_dataset(&mut self, abcd_data: &AbcdResult) -> Result<(), Error> {
        match self.surrogate_key.for_id(&abcd_data.dataset_id) {
            SurrogateKeyType::New(surrogate_key) => {
                Self::insert_dataset_metadata(
                    self.database_settings,
                    &mut self.connection,
                    self.dataset_fields.as_slice(),
                    abcd_data,
                    surrogate_key,
                )?;
                self.insert_units(abcd_data, surrogate_key)?;
            }
            SurrogateKeyType::Existing(surrogate_key) => {
                self.insert_units(abcd_data, surrogate_key)?;
            }
        }

        Ok(())
    }

    /// Insert the dataset metadata into the temporary schema
    fn insert_dataset_metadata(
        database_settings: &settings::DatabaseSettings,
        connection: &mut Client,
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
            database_settings.surrogate_key_column.as_ref(),
            database_settings.dataset_id_column.as_ref(),
            database_settings.dataset_path_column.as_ref(),
            database_settings.dataset_landing_page_column.as_ref(),
            database_settings.dataset_provider_column.as_ref(),
        ];
        values.write_field(id.to_string())?;
        values.write_field(abcd_data.dataset_id.clone())?;
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

        let copy_statement = connection.prepare(&copy_statement)?;

        let mut writer = connection.copy_in(&copy_statement)?;

        let value_string = values.into_inner()?;
        // dbg!(String::from_utf8_lossy(value_string.as_slice()));

        writer.write_all(value_string.as_slice())?;
        writer.finish()?;

        Ok(())
    }

    /// Insert the dataset units into the temporary schema
    fn insert_units(&mut self, abcd_data: &AbcdResult, id: u32) -> Result<(), Error> {
        let mut columns: Vec<String> = vec![self.database_settings.surrogate_key_column.clone()];
        columns.extend(self.unit_fields.iter().map(|field| field.hash.clone()));
        columns.push("geom".to_owned());

        let mut values = WriterBuilder::new()
            .terminator(csv::Terminator::Any(b'\n'))
            .delimiter(b'\t')
            .quote(b'"')
            .escape(b'"')
            .has_headers(false)
            .from_writer(vec![]);

        // append units one by one to tsv
        for unit_data in &abcd_data.units {
            values.write_field(&id.to_string())?; // put id first

            let mut lon = None;
            let mut lat = None;
            for field in &self.unit_fields {
                if let Some(value) = unit_data.get(&field.name) {
                    values.write_field(value.to_string())?;

                    if field.name == "/DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/CoordinatesLatLong/LongitudeDecimal" {
                        lon = Some(value);
                    } else if field.name == "/DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/CoordinatesLatLong/LatitudeDecimal" {
                        lat = Some(value);
                    }
                } else {
                    values.write_field("")?;
                }
            }

            if let (Some(lon), Some(lat)) = (lon, lat) {
                values.write_field(format!("POINT({} {})", lon, lat))?;
            } else {
                values.write_field("")?;
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

        let mut writer = self.connection.copy_in(&statement)?;
        writer.write_all(values.into_inner()?.as_slice())?;
        writer.finish()?;

        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::settings::{DatabaseSettings, Settings};
    use crate::test_utils;
    use postgres::Row;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn schema_creation_leads_to_required_tables() {
        let database_settings = retrieve_settings_from_file_and_override_schema();
        let abcd_fields = create_abcd_fields_from_json(&json!([]));

        let mut database_sink = DatabaseSink::new(&database_settings, &abcd_fields).unwrap();

        let tables = retrieve_ordered_table_names(&mut database_sink);

        assert_eq!(
            tables,
            sorted_vec(vec![
                database_settings.temp_dataset_table.clone(),
                database_settings.temp_unit_table.clone(),
                format!("{}_translation", database_settings.temp_dataset_table)
            ])
        );
    }

    #[test]
    fn schema_creation_leads_to_required_columns_in_dataset_table() {
        let database_settings = retrieve_settings_from_file_and_override_schema();
        let abcd_fields = create_abcd_fields_from_json(&json!([
            {
                "name": "/DataSets/DataSet/TechnicalContacts/TechnicalContact/Name",
                "numeric": false,
                "vatMandatory": false,
                "gfbioMandatory": true,
                "globalField": true,
                "unit": ""
            },
            {
                "name": "/DataSets/DataSet/Metadata/Description/Representation/Title",
                "numeric": false,
                "vatMandatory": false,
                "gfbioMandatory": true,
                "globalField": true,
                "unit": ""
            },
            {
                "name": "/DataSets/DataSet/Metadata/Description/Representation/URI",
                "numeric": false,
                "vatMandatory": false,
                "gfbioMandatory": true,
                "globalField": true,
                "unit": ""
            },
        ]));

        let mut database_sink = DatabaseSink::new(&database_settings, &abcd_fields).unwrap();

        let dataset_table_columns = retrieve_ordered_table_column_names(
            &mut database_sink,
            &database_settings.temp_dataset_table,
        );

        let dataset_columns = extract_dataset_fields(&abcd_fields)
            .iter()
            .map(|field| field.hash.clone())
            .chain(vec![
                database_settings.surrogate_key_column.clone(),
                "dataset_id".to_string(),
                "dataset_landing_page".to_string(),
                "dataset_path".to_string(),
                "dataset_provider".to_string(),
            ])
            .collect::<Vec<_>>();

        assert!(!dataset_columns.is_empty());
        assert_eq!(dataset_table_columns, sorted_vec(dataset_columns));
    }

    #[test]
    fn schema_creation_leads_to_required_columns_in_unit_table() {
        let database_settings = retrieve_settings_from_file_and_override_schema();
        let abcd_fields = create_abcd_fields_from_json(&json!([
            {
                "name": "/DataSets/DataSet/Units/Unit/UnitID",
                "numeric": false,
                "vatMandatory": false,
                "gfbioMandatory": true,
                "globalField": false,
                "unit": ""
            },
            {
                "name": "/DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/CoordinatesLatLong/LongitudeDecimal",
                "numeric": true,
                "vatMandatory": true,
                "gfbioMandatory": true,
                "globalField": false,
                "unit": "°"
            },
            {
                "name": "/DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/CoordinatesLatLong/LatitudeDecimal",
                "numeric": true,
                "vatMandatory": true,
                "gfbioMandatory": true,
                "globalField": false,
                "unit": "°"
            },
            {
                "name": "/DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/CoordinatesLatLong/SpatialDatum",
                "numeric": false,
                "vatMandatory": false,
                "gfbioMandatory": true,
                "globalField": false,
                "unit": ""
            }
        ]));

        let mut database_sink = DatabaseSink::new(&database_settings, &abcd_fields).unwrap();

        let dataset_table_columns = retrieve_ordered_table_column_names(
            &mut database_sink,
            &database_settings.temp_unit_table,
        );

        let unit_columns = extract_unit_fields(&abcd_fields)
            .iter()
            .map(|field| field.hash.clone())
            .chain(vec![database_settings.surrogate_key_column.clone()])
            .chain(vec!["geom".to_owned()])
            .collect::<Vec<_>>();

        assert!(!unit_columns.is_empty());
        assert_eq!(dataset_table_columns, sorted_vec(unit_columns));
    }

    #[test]
    fn translation_table_contains_entries() {
        let database_settings = retrieve_settings_from_file_and_override_schema();
        let abcd_fields = create_abcd_fields_from_json(&json!([
            {
                "name": "/DataSets/DataSet/TechnicalContacts/TechnicalContact/Name",
                "numeric": false,
                "vatMandatory": false,
                "gfbioMandatory": true,
                "globalField": true,
                "unit": ""
            },
            {
                "name": "/DataSets/DataSet/Metadata/Description/Representation/Title",
                "numeric": false,
                "vatMandatory": false,
                "gfbioMandatory": true,
                "globalField": true,
                "unit": ""
            },
            {
                "name": "/DataSets/DataSet/Metadata/Description/Representation/URI",
                "numeric": false,
                "vatMandatory": false,
                "gfbioMandatory": true,
                "globalField": true,
                "unit": ""
            },
        ]));

        let mut database_sink = DatabaseSink::new(&database_settings, &abcd_fields).unwrap();

        let expected_translation_table_columns = vec![
            "/DataSets/DataSet/TechnicalContacts/TechnicalContact/Name",
            "/DataSets/DataSet/Metadata/Description/Representation/Title",
            "/DataSets/DataSet/Metadata/Description/Representation/URI",
        ];

        let queried_translation_table_columns =
            retrieve_translation_table_keys(&database_settings, &mut database_sink);

        assert_eq!(
            sorted_vec(expected_translation_table_columns),
            sorted_vec(queried_translation_table_columns)
        );
    }

    #[test]
    fn translation_table_entries_match_table_columns() {
        let database_settings = retrieve_settings_from_file_and_override_schema();
        let abcd_fields = create_abcd_fields_from_json(&json!([
            {
                "name": "/DataSets/DataSet/TechnicalContacts/TechnicalContact/Name",
                "numeric": false,
                "vatMandatory": false,
                "gfbioMandatory": true,
                "globalField": true,
                "unit": ""
            },
            {
                "name": "/DataSets/DataSet/Metadata/Description/Representation/Title",
                "numeric": false,
                "vatMandatory": false,
                "gfbioMandatory": true,
                "globalField": true,
                "unit": ""
            },
            {
                "name": "/DataSets/DataSet/Units/Unit/UnitID",
                "numeric": false,
                "vatMandatory": false,
                "gfbioMandatory": true,
                "globalField": false,
                "unit": ""
            },
        ]));

        let mut database_sink = DatabaseSink::new(&database_settings, &abcd_fields).unwrap();

        let dataset_table_columns = retrieve_ordered_table_column_names(
            &mut database_sink,
            &database_settings.temp_dataset_table,
        );
        let unit_table_columns = retrieve_ordered_table_column_names(
            &mut database_sink,
            &database_settings.temp_unit_table,
        );

        let translation_table_values =
            retrieve_translation_table_values(&database_settings, &mut database_sink);

        for column_name in translation_table_values {
            assert!(
                dataset_table_columns.contains(&column_name)
                    || unit_table_columns.contains(&column_name)
            );
        }
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn dataset_table_contains_entry_after_insert() {
        let database_settings = retrieve_settings_from_file_and_override_schema();
        let abcd_fields = create_abcd_fields_from_json(&json!([
            {
                "name": "DS_TEXT",
                "numeric": false,
                "vatMandatory": false,
                "gfbioMandatory": true,
                "globalField": true,
                "unit": ""
            },
            {
                "name": "DS_NUM",
                "numeric": true,
                "vatMandatory": false,
                "gfbioMandatory": true,
                "globalField": true,
                "unit": ""
            },
            {
                "name": "UNIT_TEXT",
                "numeric": false,
                "vatMandatory": false,
                "gfbioMandatory": true,
                "globalField": false,
                "unit": ""
            },
            {
                "name": "UNIT_NUM",
                "numeric": true,
                "vatMandatory": false,
                "gfbioMandatory": true,
                "globalField": false,
                "unit": ""
            },
        ]));

        let mut database_sink = DatabaseSink::new(&database_settings, &abcd_fields).unwrap();

        database_sink
            .insert_dataset(&AbcdResult {
                dataset_id: "TEST_ID".to_string(),
                dataset_path: "TEST_PATH".to_string(),
                landing_page: "TEST_LANDING_PAGE".to_string(),
                provider_name: "TEST_PROVIDER".to_string(),
                dataset: {
                    let mut values = HashMap::new();
                    values.insert("DS_TEXT".into(), "FOOBAR".into());
                    values.insert("DS_NUM".into(), 42.0.into());
                    values
                },
                units: vec![
                    {
                        let mut values = HashMap::new();
                        values.insert("UNIT_TEXT".into(), "FOO".into());
                        values.insert("UNIT_NUM".into(), 13.0.into());
                        values
                    },
                    {
                        let mut values = HashMap::new();
                        values.insert("UNIT_TEXT".into(), "BAR".into());
                        values.insert("UNIT_NUM".into(), 37.0.into());
                        values
                    },
                ],
            })
            .unwrap();

        assert_eq!(
            1,
            number_of_entries(&mut database_sink, &database_settings.temp_dataset_table)
        );
        assert_eq!(
            2,
            number_of_entries(&mut database_sink, &database_settings.temp_unit_table)
        );

        let dataset_result =
            retrieve_rows(&mut database_sink, &database_settings.temp_dataset_table);

        let dataset = dataset_result.first().unwrap();
        assert_eq!(
            "TEST_ID",
            dataset.get::<_, &str>(database_settings.dataset_id_column.as_str())
        );
        assert_eq!(
            "TEST_PATH",
            dataset.get::<_, &str>(database_settings.dataset_path_column.as_str())
        );
        assert_eq!(
            "TEST_LANDING_PAGE",
            dataset.get::<_, &str>(database_settings.dataset_landing_page_column.as_str())
        );
        assert_eq!(
            "TEST_PROVIDER",
            dataset.get::<_, &str>(database_settings.dataset_provider_column.as_str())
        );
        assert_eq!(
            "FOOBAR",
            dataset.get::<_, &str>(Field::new("DS_TEXT").hash.as_str())
        );
        assert_eq!(
            42.0,
            dataset.get::<_, f64>(Field::new("DS_NUM").hash.as_str())
        );

        let unit_result = retrieve_rows(&mut database_sink, &database_settings.temp_unit_table);

        let unit1 = unit_result.get(0).unwrap();
        assert_eq!(
            "FOO",
            unit1.get::<_, &str>(Field::new("UNIT_TEXT").hash.as_str())
        );
        assert_eq!(
            13.0,
            unit1.get::<_, f64>(Field::new("UNIT_NUM").hash.as_str())
        );

        let unit2 = unit_result.get(1).unwrap();
        assert_eq!(
            "BAR",
            unit2.get::<_, &str>(Field::new("UNIT_TEXT").hash.as_str())
        );
        assert_eq!(
            37.0,
            unit2.get::<_, f64>(Field::new("UNIT_NUM").hash.as_str())
        );
    }

    #[test]
    fn second_insert_of_same_dataset_does_not_lead_to_second_entry_in_dataset_table() {
        let database_settings = retrieve_settings_from_file_and_override_schema();
        let abcd_fields = create_abcd_fields_from_json(&json!([
            {
                "name": "DS_TEXT",
                "numeric": false,
                "vatMandatory": false,
                "gfbioMandatory": true,
                "globalField": true,
                "unit": ""
            },
            {
                "name": "DS_NUM",
                "numeric": true,
                "vatMandatory": false,
                "gfbioMandatory": true,
                "globalField": true,
                "unit": ""
            },
            {
                "name": "UNIT_TEXT",
                "numeric": false,
                "vatMandatory": false,
                "gfbioMandatory": true,
                "globalField": false,
                "unit": ""
            },
            {
                "name": "UNIT_NUM",
                "numeric": true,
                "vatMandatory": false,
                "gfbioMandatory": true,
                "globalField": false,
                "unit": ""
            },
        ]));

        let mut database_sink = DatabaseSink::new(&database_settings, &abcd_fields).unwrap();

        database_sink
            .insert_dataset(&AbcdResult {
                dataset_id: "TEST_ID".to_string(),
                dataset_path: "TEST_PATH".to_string(),
                landing_page: "TEST_LANDING_PAGE".to_string(),
                provider_name: "TEST_PROVIDER".to_string(),
                dataset: {
                    let mut values = HashMap::new();
                    values.insert("DS_TEXT".into(), "FOOBAR".into());
                    values.insert("DS_NUM".into(), 42.0.into());
                    values
                },
                units: vec![{
                    let mut values = HashMap::new();
                    values.insert("UNIT_TEXT".into(), "FOO".into());
                    values.insert("UNIT_NUM".into(), 13.0.into());
                    values
                }],
            })
            .unwrap();

        database_sink
            .insert_dataset(&AbcdResult {
                dataset_id: "TEST_ID".to_string(),
                dataset_path: "TEST_PATH".to_string(),
                landing_page: "TEST_LANDING_PAGE".to_string(),
                provider_name: "TEST_PROVIDER".to_string(),
                dataset: {
                    let mut values = HashMap::new();
                    values.insert("DS_TEXT".into(), "FOOBAR".into());
                    values.insert("DS_NUM".into(), 42.0.into());
                    values
                },
                units: vec![{
                    let mut values = HashMap::new();
                    values.insert("UNIT_TEXT".into(), "BAR".into());
                    values.insert("UNIT_NUM".into(), 37.0.into());
                    values
                }],
            })
            .unwrap();

        assert_eq!(
            1,
            number_of_entries(&mut database_sink, &database_settings.temp_dataset_table)
        );
        assert_eq!(
            2,
            number_of_entries(&mut database_sink, &database_settings.temp_unit_table)
        );
    }

    #[test]
    fn correct_tables_after_schema_migration() {
        let mut database_settings = retrieve_settings_from_file_and_override_schema();
        database_settings.unit_indexed_columns = vec![];

        let abcd_fields = create_abcd_fields_from_json(&json!([]));

        let mut database_sink = DatabaseSink::new(&database_settings, &abcd_fields).unwrap();

        database_sink
            .insert_dataset(&AbcdResult {
                dataset_id: "TEST_ID".to_string(),
                dataset_path: "TEST_PATH".to_string(),
                landing_page: "TEST_LANDING_PAGE".to_string(),
                provider_name: "TEST_PROVIDER".to_string(),
                dataset: Default::default(),
                units: vec![],
            })
            .unwrap();

        database_sink.migrate_schema().unwrap();

        let tables = retrieve_ordered_table_names(&mut database_sink);

        assert_eq!(
            tables,
            sorted_vec(vec![
                database_settings.dataset_table.clone(),
                database_settings.unit_table.clone(),
                format!("{}_translation", database_settings.dataset_table),
                database_settings.listing_view.clone(),
            ])
        );
    }

    #[test]
    fn listing_view_contains_entry_after_migration() {
        let mut database_settings = retrieve_settings_from_file_and_override_schema();
        database_settings.unit_indexed_columns = vec![];

        let abcd_fields = create_abcd_fields_from_json(&json!([
            {
                "name": "/DataSets/DataSet/Metadata/Description/Representation/Title",
                "numeric": false,
                "vatMandatory": false,
                "gfbioMandatory": true,
                "globalField": true,
                "unit": ""
            },
            {
                "name": "/DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/CoordinatesLatLong/LatitudeDecimal",
                "numeric": true,
                "vatMandatory": false,
                "gfbioMandatory": true,
                "globalField": false,
                "unit": ""
            },
            {
                "name": "/DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/CoordinatesLatLong/LongitudeDecimal",
                "numeric": true,
                "vatMandatory": false,
                "gfbioMandatory": true,
                "globalField": false,
                "unit": ""
            },
        ]));

        let mut database_sink = DatabaseSink::new(&database_settings, &abcd_fields).unwrap();

        database_sink
            .insert_dataset(&AbcdResult {
                dataset_id: "TEST_ID".to_string(),
                dataset_path: "TEST_PATH".to_string(),
                landing_page: "TEST_LANDING_PAGE".to_string(),
                provider_name: "TEST_PROVIDER".to_string(),
                dataset: {
                        let mut values = HashMap::new();
                        values.insert("/DataSets/DataSet/Metadata/Description/Representation/Title".into(), "FOOBAR".into());
                        values
                },
                units: vec![
                    {
                        let mut values = HashMap::new();
                        values.insert("/DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/CoordinatesLatLong/LatitudeDecimal".into(), 10.0.into());
                        values.insert("/DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/CoordinatesLatLong/LongitudeDecimal".into(), 20.0.into());
                        values
                    },
                ],
            })
            .unwrap();

        database_sink.migrate_schema().unwrap();

        retrieve_ordered_table_column_names(&mut database_sink, &database_settings.listing_view);

        let statement = database_sink
            .connection
            .prepare(&format!(
                r#"SELECT * FROM pg_temp.{LISTING_VIEW}"#,
                LISTING_VIEW = database_settings.listing_view
            ))
            .unwrap();

        let rows = database_sink.connection.query(&statement, &[]).unwrap();

        assert_eq!(rows.len(), 1);

        let row = rows.first().unwrap();
        assert_eq!(row.get::<_, &str>("dataset"), "FOOBAR");
        assert_eq!(row.get::<_, &str>("id"), "TEST_ID");
        assert_eq!(row.get::<_, &str>("link"), "TEST_LANDING_PAGE");
        assert_eq!(row.get::<_, &str>("provider"), "TEST_PROVIDER");
        assert!(row.get::<_, bool>("isGeoReferenced"));
    }

    fn retrieve_rows(database_sink: &mut DatabaseSink, table_name: &str) -> Vec<Row> {
        let statement = database_sink
            .connection
            .prepare(&format!(
                r#"SELECT * FROM pg_temp.{TABLE}"#,
                TABLE = table_name,
            ))
            .unwrap();

        database_sink.connection.query(&statement, &[]).unwrap()
    }

    fn number_of_entries(database_sink: &mut DatabaseSink, table_name: &str) -> i32 {
        let statement = database_sink
            .connection
            .prepare(&format!(
                "select count(*)::integer as total from pg_temp.{}",
                table_name
            ))
            .unwrap();

        database_sink
            .connection
            .query(&statement, &[])
            .unwrap()
            .first()
            .unwrap()
            .get::<_, i32>("total")
    }

    fn retrieve_translation_table_keys(
        database_settings: &DatabaseSettings,
        database_sink: &mut DatabaseSink,
    ) -> Vec<String> {
        let statement = database_sink
            .connection
            .prepare(&format!(
                "select name from pg_temp.{}_translation;",
                database_settings.temp_dataset_table,
            ))
            .unwrap();

        let rows = database_sink.connection.query(&statement, &[]).unwrap();

        sorted_vec(
            rows.iter()
                .map(|row| row.get::<_, String>("name"))
                .collect::<Vec<String>>(),
        )
    }

    fn retrieve_translation_table_values(
        database_settings: &DatabaseSettings,
        database_sink: &mut DatabaseSink,
    ) -> Vec<String> {
        let statement = database_sink
            .connection
            .prepare(&format!(
                "select hash from pg_temp.{}_translation;",
                database_settings.temp_dataset_table,
            ))
            .unwrap();

        let rows = database_sink.connection.query(&statement, &[]).unwrap();

        sorted_vec(
            rows.iter()
                .map(|row| row.get::<_, String>("hash"))
                .collect::<Vec<String>>(),
        )
    }

    fn sorted_vec<T>(mut vec: Vec<T>) -> Vec<T>
    where
        T: Ord,
    {
        vec.sort();
        vec
    }

    fn retrieve_ordered_table_names(database_sink: &mut DatabaseSink) -> Vec<String> {
        let mut tables = database_sink
            .connection
            .query(
                r#"
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = (SELECT nspname FROM pg_namespace WHERE oid = pg_my_temp_schema())
                    ;
                "#,
                &[],
            )
            .unwrap()
            .iter()
            .map(|row| row.get("table_name"))
            .collect::<Vec<String>>();

        tables.sort();

        tables
    }

    fn retrieve_ordered_table_column_names(
        database_sink: &mut DatabaseSink,
        table_name: &str,
    ) -> Vec<String> {
        let mut tables = database_sink
            .connection
            .query(
                r#"
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = (SELECT nspname FROM pg_namespace WHERE oid = pg_my_temp_schema())
                      AND table_name = $1
                    ;
                "#,
                &[&table_name.to_string()],
            )
            .unwrap()
            .iter()
            .map(|row| row.get("column_name"))
            .collect::<Vec<String>>();

        tables.sort();

        tables
    }

    fn retrieve_settings_from_file_and_override_schema() -> DatabaseSettings {
        let mut settings = Settings::new(None).unwrap().database;
        settings.schema = "pg_temp".into();
        settings
    }

    fn create_abcd_fields_from_json(json: &serde_json::Value) -> AbcdFields {
        let fields_file = test_utils::create_temp_file(&json.to_string());

        AbcdFields::from_path(&fields_file).expect("Unable to create ABCD Fields Spec")
    }

    fn extract_dataset_fields(abcd_fields: &AbcdFields) -> Vec<Field> {
        abcd_fields
            .into_iter()
            .filter(|field| field.global_field)
            .map(|field| field.name.as_ref())
            .map(Field::new)
            .collect()
    }

    fn extract_unit_fields(abcd_fields: &AbcdFields) -> Vec<Field> {
        abcd_fields
            .into_iter()
            .filter(|field| !field.global_field)
            .map(|field| field.name.as_ref())
            .map(Field::new)
            .collect()
    }
}
