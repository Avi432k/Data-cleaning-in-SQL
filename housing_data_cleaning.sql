SELECT *
FROM `nashville housing data for data cleaning`;

-- STANDARDISE SaleDate COLUMN

SELECT SaleDate,
STR_TO_DATE(SaleDate, '%M %d, %Y')
FROM `nashville housing data for data cleaning`;

UPDATE `nashville housing data for data cleaning`
SET SaleDate = STR_TO_DATE(SaleDate, '%M %d, %Y');

ALTER TABLE `nashville housing data for data cleaning`
MODIFY SaleDate DATE;

-- POPULATE PROPERTY ADDRESS

SELECT ParcelID, PropertyAddress
FROM `nashville housing data for data cleaning`
WHERE PropertyAddress IS NULL
ORDER BY ParcelID;

SELECT t1.ParcelID, t2.ParcelID, t1.PropertyAddress, t2.PropertyAddress
FROM `nashville housing data for data cleaning` t1
JOIN `nashville housing data for data cleaning` t2
	ON t1.ParcelID = t2.ParcelID
WHERE t1.PropertyAddress IS NULL
AND t2.PropertyAddress IS NOT NULL
ORDER BY t1.ParcelID;

UPDATE `nashville housing data for data cleaning` t1
JOIN `nashville housing data for data cleaning` t2
	ON t1.ParcelID = t2.ParcelID
SET t1.PropertyAddress = t2.PropertyAddress
WHERE t1.PropertyAddress IS NULL
AND t2.PropertyAddress IS NOT NULL;

-- BREAKING OUT ADDRESS INTO INDIVIDUAL COLUMNS (ADDRESS, CITY, STATE)

SELECT PropertyAddress, PropertySplitAddress, PropertySplitCity
FROM `nashville housing data for data cleaning`;

SELECT 
SUBSTRING(PropertyAddress, 1, LOCATE(',', PropertyAddress) - 1) AS Address,
SUBSTRING(PropertyAddress, LOCATE(',', PropertyAddress) + 1) AS City
FROM `nashville housing data for data cleaning`;

ALTER TABLE `nashville housing data for data cleaning`
ADD PropertySplitAddress VARCHAR(255);

ALTER TABLE `nashville housing data for data cleaning` 
ADD PropertySplitCity VARCHAR(255);

UPDATE `nashville housing data for data cleaning`
SET PropertySplitAddress = SUBSTRING(PropertyAddress, 1, LOCATE(',', PropertyAddress) - 1);

UPDATE `nashville housing data for data cleaning`
SET PropertySplitCity = SUBSTRING(PropertyAddress, LOCATE(',', PropertyAddress) + 1);

SELECT OwnerAddress, OwnerSplitAddress, OwnerSplitCity, OwnerSplitState
FROM `nashville housing data for data cleaning`;

SELECT
OwnerAddress,
SUBSTRING_INDEX(OwnerAddress, ',', 1) AS Address,
SUBSTRING_INDEX(SUBSTRING_INDEX(OwnerAddress, ',', -2), ',', 1) AS City,
SUBSTRING_INDEX(OwnerAddress, ',', -1) AS State
FROM `nashville housing data for data cleaning`;

ALTER TABLE `nashville housing data for data cleaning`
ADD OwnerSplitAddress NVARCHAR(255);

ALTER TABLE `nashville housing data for data cleaning`
ADD OwnerSplitCity NVARCHAR(255);

ALTER TABLE `nashville housing data for data cleaning`
ADD OwnerSplitState NVARCHAR(255);

UPDATE `nashville housing data for data cleaning`
SET OwnerSplitAddress = SUBSTRING_INDEX(OwnerAddress, ',', 1);

UPDATE `nashville housing data for data cleaning`
SET OwnerSplitCity = SUBSTRING_INDEX(SUBSTRING_INDEX(OwnerAddress, ',', -2), ',', 1);

UPDATE `nashville housing data for data cleaning`
SET OwnerSplitState = SUBSTRING_INDEX(OwnerAddress, ',', -1);

-- CHANGE Y AND N TO YES AND NO IN SOLD AS VACANT COLUMN

SELECT DISTINCT SoldAsVacant
FROM `nashville housing data for data cleaning`;

UPDATE `nashville housing data for data cleaning`
SET SoldAsVacant = 'No' WHERE SoldAsVacant = 'N'; 

UPDATE `nashville housing data for data cleaning`
SET SoldAsVacant = 'Yes' WHERE SoldAsVacant = 'Y'; 

-- REMOVE DUPLICATES

SELECT DISTINCT UniqueID, 
COUNT(UniqueID) AS IDcount
FROM `nashville housing data for data cleaning`
GROUP BY UniqueID
HAVING IDcount > 1;

SELECT *,
ROW_NUMBER() OVER(PARTITION BY ParcelID, LandUse, PropertyAddress, SaleDate, SalePrice, LegalReference, SoldAsVacant, OwnerName, OwnerAddress, Acreage, TaxDistrict, LandValue, BuildingValue) AS row_num
FROM `nashville housing data for data cleaning`;

WITH Duplicates AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY ParcelID, LandUse, PropertyAddress, LegalReference, FullBath, HalfBath, Bedrooms) AS row_num
FROM `nashville housing data for data cleaning`
)
SELECT *,
row_num
FROM Duplicates
WHERE row_num > 1;

CREATE TABLE Housing_data2
  (
  `UniqueID` int DEFAULT NULL,
  `ParcelID` text,
  `LandUse` text,
  `PropertyAddress` text,
  `SaleDate` date DEFAULT NULL,
  `SalePrice` int DEFAULT NULL,
  `LegalReference` text,
  `SoldAsVacant` text,
  `OwnerName` text,
  `OwnerAddress` text,
  `Acreage` double DEFAULT NULL,
  `TaxDistrict` text,
  `LandValue` int DEFAULT NULL,
  `BuildingValue` int DEFAULT NULL,
  `TotalValue` int DEFAULT NULL,
  `YearBuilt` int DEFAULT NULL,
  `Bedrooms` int DEFAULT NULL,
  `FullBath` int DEFAULT NULL,
  `HalfBath` int DEFAULT NULL,
  `SaleDateConverted` date DEFAULT NULL,
  `PropertySplitAddress` varchar(255) DEFAULT NULL,
  `PropertySplitCity` varchar(255) DEFAULT NULL,
  `OwnerSplitAddress` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `OwnerSplitCity` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `OwnerSplitState` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `row_num` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT Housing_data2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY ParcelID, LandUse, PropertyAddress, LegalReference, FullBath, HalfBath, Bedrooms) AS row_num
FROM `nashville housing data for data cleaning`;

SELECT *
FROM Housing_data2;

DELETE
FROM Housing_data2
WHERE row_num > 1;


-- DELETE UNUSED COLUMNS

ALTER TABLE Housing_data2
DROP COLUMN PropertyAddress, 
DROP COLUMN OwnerAddress,
DROP COLUMN row_num,
DROP COLUMN SaleDate;

SELECT *
FROM Housing_data2;

ALTER TABLE Housing_data2
RENAME COLUMN PropertySplitAddress TO PropertyAddress,
RENAME COLUMN PropertySplitCity TO PropertyCity,
RENAME COLUMN OwnerSplitAddress TO OwnerAddress,
RENAME COLUMN OwnerSplitCity TO OwnerCity,
RENAME COLUMN OwnerSplitState TO OwnerState;

-- FIX MISTAKE OF ACCIDENTLY DELETING THE PROPERTY ADDRRESS AND OWNERADDRESS COLUMNS

CREATE TABLE Housing_data3 (
  `UniqueID` int DEFAULT NULL,
  `ParcelID` text,
  `LandUse` text,
  `PropertyAddress` text,
  `SaleDate` date DEFAULT NULL,
  `SalePrice` int DEFAULT NULL,
  `LegalReference` text,
  `SoldAsVacant` text,
  `OwnerName` text,
  `OwnerAddress` text,
  `Acreage` double DEFAULT NULL,
  `TaxDistrict` text,
  `LandValue` int DEFAULT NULL,
  `BuildingValue` int DEFAULT NULL,
  `TotalValue` int DEFAULT NULL,
  `YearBuilt` int DEFAULT NULL,
  `Bedrooms` int DEFAULT NULL,
  `FullBath` int DEFAULT NULL,
  `HalfBath` int DEFAULT NULL,
  `SaleDateConverted` date DEFAULT NULL,
  `PropertySplitAddress` varchar(255) DEFAULT NULL,
  `PropertySplitCity` varchar(255) DEFAULT NULL,
  `OwnerSplitAddress` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `OwnerSplitCity` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `OwnerSplitState` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `row_num` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT Housing_data3
SELECT *,
ROW_NUMBER() OVER(PARTITION BY ParcelID, LandUse, PropertyAddress, LegalReference, FullBath, HalfBath, Bedrooms) AS row_num
FROM `nashville housing data for data cleaning`;

SELECT *
FROM Housing_data3;

DELETE
FROM Housing_data3
WHERE row_num > 1;

ALTER TABLE Housing_data3
DROP COLUMN PropertyAddress, 
DROP COLUMN OwnerAddress,
DROP COLUMN row_num,
DROP COLUMN SaleDateConverted;

SELECT *
FROM Housing_data3;

ALTER TABLE Housing_data3
RENAME COLUMN PropertySplitAddress TO PropertyAddress,
RENAME COLUMN PropertySplitCity TO PropertyCity,
RENAME COLUMN OwnerSplitAddress TO OwnerAddress,
RENAME COLUMN OwnerSplitCity TO OwnerCity,
RENAME COLUMN OwnerSplitState TO OwnerState;


 



