pub struct Dataset {
    provider: String,
    guid: String, // "DataSets/DataSet/DatasetGUID"
    title: String, // "DataSets/DataSet/Metadata/Description/Representation/Title"
    citation: String, // "DataSets/DataSet/Metadata/IPRStatements/Citations/Citation/Text"
    license: String, // "DataSets/DataSet/Metadata/IPRStatements/Licenses/License/Text"
    uri: String, // "DataSets/DataSet/Metadata/Description/Representation/URI"
    is_geo_referenced: bool,
    units: Vec<Unit>,
}

impl Dataset {
    pub fn new(guid: String,
               title: String,
               provider: String,
               citation: String,
               license: String,
               link: String,) -> Self {
        Self {
            provider,
            guid,
            title,
            citation,
            license,
            uri,
            is_geo_referenced: false,
            units: Vec::new(),
        }
    }

    pub fn set_is_geo_referenced(&mut self, is_geo_referenced: bool) {
        self.is_geo_referenced = is_geo_referenced;
    }

    pub fn add_unit(&mut self, unit: Unit) {
        self.units.push(unit);
    }
}

pub struct Unit {
    guid: String, // "DataSets/DataSet/Units/Unit/UnitID"
    longitude_decimal: f64, // "DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/LongitudeDecimal"
    latitude_decimal: f64, // "DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/LatitudeDecimal"
}

impl Unit {
    pub fn new(guid: String, longitude_decimal: f64, latitude_decimal: f64,) -> Self {
        Self {
            guid,
            longitude_decimal,
            latitude_decimal,
        }
    }
}
