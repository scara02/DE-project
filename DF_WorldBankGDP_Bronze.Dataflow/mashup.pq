[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared bronze_gdp = let
  Source = Json.Document(Web.Contents("https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD?format=json&per_page=100")),
  #"Converted to table" = Table.FromList(Source, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  Navigation = #"Converted to table"{1}[Column1],
  #"Converted to table 1" = Table.FromList(Navigation, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  #"Expanded Column1" = Table.ExpandRecordColumn(#"Converted to table 1", "Column1", {"date", "value"}, {"date", "value"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded Column1", {{"date", "year"}, {"value", "gdp_usd"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns", {{"year", type text}, {"gdp_usd", type number}})
in
  #"Changed column type";
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "5d2135e8-4491-4ded-a6fa-a0347daedcc8"]}[Data]{[lakehouseId = "7bc6df61-fa1d-4d3d-867d-e9df41248645"]}[Data];
