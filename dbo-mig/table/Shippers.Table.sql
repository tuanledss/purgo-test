
CREATE TABLE Shippers (
  ShipperID INT NOT NULL, 
  CompanyName STRING NOT NULL, 
  Phone STRING, 
  PRIMARY KEY (ShipperID)
)
USING DELTA;
