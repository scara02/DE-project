[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared bronze_ecb = let
  Source = Csv.Document(Web.Contents("https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata"), [Delimiter = ",", Columns = 32, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"KEY", type text}, {"FREQ", type text}, {"CURRENCY", type text}, {"CURRENCY_DENOM", type text}, {"EXR_TYPE", type text}, {"EXR_SUFFIX", type text}, {"TIME_PERIOD", type date}, {"OBS_VALUE", type number}, {"OBS_STATUS", type text}, {"OBS_CONF", type text}, {"OBS_PRE_BREAK", type text}, {"OBS_COM", type text}, {"TIME_FORMAT", type duration}, {"BREAKS", type text}, {"COLLECTION", type text}, {"COMPILING_ORG", type text}, {"DISS_ORG", type text}, {"DOM_SER_IDS", type text}, {"PUBL_ECB", type text}, {"PUBL_MU", type text}, {"PUBL_PUBLIC", type text}, {"UNIT_INDEX_BASE", type text}, {"COMPILATION", type text}, {"COVERAGE", type text}, {"DECIMALS", Int64.Type}, {"NAT_TITLE", type text}, {"SOURCE_AGENCY", type text}, {"SOURCE_PUB", type text}, {"TITLE", type text}, {"TITLE_COMPL", type text}, {"UNIT", type text}, {"UNIT_MULT", Int64.Type}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type", {"KEY", "FREQ", "CURRENCY", "CURRENCY_DENOM", "EXR_TYPE", "EXR_SUFFIX", "OBS_STATUS", "OBS_CONF", "OBS_PRE_BREAK", "OBS_COM", "TIME_FORMAT", "BREAKS", "COLLECTION", "COMPILING_ORG", "DISS_ORG", "DOM_SER_IDS", "PUBL_ECB", "PUBL_MU", "PUBL_PUBLIC", "UNIT_INDEX_BASE", "COMPILATION", "COVERAGE", "DECIMALS", "NAT_TITLE", "SOURCE_AGENCY", "SOURCE_PUB", "TITLE", "TITLE_COMPL", "UNIT", "UNIT_MULT"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"TIME_PERIOD", "fx_date"}, {"OBS_VALUE", "usd_eur"}})
in
  #"Renamed columns";
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "5d2135e8-4491-4ded-a6fa-a0347daedcc8"]}[Data]{[lakehouseId = "7bc6df61-fa1d-4d3d-867d-e9df41248645"]}[Data];
