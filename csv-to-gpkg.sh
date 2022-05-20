#!/bin/sh

ogr2ogr \
    -nlt POINT \
    -a_srs EPSG:4326 \
    -oo X_POSSIBLE_NAMES=/DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/CoordinatesLatLong/LongitudeDecimal \
    -oo Y_POSSIBLE_NAMES=/DataSets/DataSet/Units/Unit/Gathering/SiteCoordinateSets/SiteCoordinates/CoordinatesLatLong/LatitudeDecimal \
    -oo AUTODETECT_TYPE=YES \
    -oo KEEP_GEOM_COLUMNS=NO \
    -sql "select \"/DataSets/DataSet/Units/Unit/Gathering/DateTime/ISODateTimeBegin\" as Date, \"/DataSets/DataSet/Units/Unit/Identifications/Identification/Result/TaxonIdentified/ScientificName/FullScientificNameString\" as Species from out" \
    -overwrite \
    -f GPKG output.gpkg \
    out.csv
