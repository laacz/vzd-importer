#!/usr/bin/env bash

# Download all your data from https://data.gov.lv/dati/lv/dataset/kadastra-informacijas-sistemas-atverti-telpiskie-dati
# into data/kadastrs/ folder
# Script does the following:
# 1. Downloads all data
# 2. Creates a single shapefile with all the layers
# 3. Imports all the layers into PostGIS

# Download

echo "Getting file list"
FILES=$(curl -s https://data.gov.lv/dati/lv/dataset/b28f0eed-73b0-4e44-94e7-b04b11bf0b69.jsonld | jq -r '."@graph"[]."dcat:accessURL"."@id" | select(. != null)')

rm -rf data/kadastrs && mkdir -p data/kadastrs

FILES_ARR=( $FILES )
TOTAL_FILES=${#FILES_ARR[@]}
CURRENT_FILE=0

for FILE in $FILES
do
    CURRENT_FILE=$((CURRENT_FILE+1))
    echo -n "Downloading $CURRENT_FILE of $TOTAL_FILES file(s) - ${FILE##*/} ... "
    curl "$FILE" -s -o data/kadastrs/${FILE##*/}
    echo -n " unzipping ... "
    unzip -qq -o data/kadastrs/${FILE##*/} -d data/kadastrs/
    echo "ok"
done

# Import
APPEND=0
LAYERS="KKCadastralGroup KKBuilding KKEngineeringStructurePoly KKParcel KKParcelBorderPoint KKParcelError KKParcelPart KKSurveyingStatus KKWayRestriction"
DB=vzd

for type in $LAYERS; do
    APPEND=0
    rm -f "data/kadastrs/$type*";
    target_file="data/kadastrs/$type.shp"
    target_layer=$(echo "$type" | tr '[:upper:]' '[:lower:]')

    for file in data/kadastrs/**/"$type".shp; do
        if [ "$APPEND" == 0 ]; then
            echo -n "Create $target_file "
            ogr2ogr -f 'ESRI Shapefile' "$target_file" "$file" -lco ENCODING=UTF-8
            APPEND=1
        else
            echo -n "Update $target_file "
            ogr2ogr -f 'ESRI Shapefile' -update -append "$target_file" "$file" -nln "$target_layer"
        fi
        echo "(${target_file%.shp}; $file)"
    done
    shp2pgsql -s 3059:4326 -I -d "$target_file" | psql -q "$DB"
done
