package com.github.laxminarayannsharma;

import java.util.*;


public class ExportPivotDataService {

    private static final String TOTAL = "Total"; // Final Row Total
    private static final String EIGHT_TILDE_SYMBOLS = "~~~~~~~~"; // Final Column Total
    private static final String COLUMN = "column";
    private static final String ROW = "row";

    public HashMap<Integer, HashMap<Integer, Object>> exportPivotData(HashMap<String, String> headerMap, // Headers for columns
                                                                      List<HashMap<String, Object>> getResultHashMap, //Result
                                                                      HashMap<String, Object> flexiblePivotDTO, // For Sequence
                                                                      int pivotRowCount // Default row count for pivot
    ) {
        HashMap<Integer, HashMap<Integer, Object>> exportHashMapData = new HashMap<>();
        int pivotColumnRowCount = pivotRowCount;

        Set<String> columnHeadingKeySet = new LinkedHashSet<>((List<String>) flexiblePivotDTO.get(COLUMN));
        Set<String> rowHeadingKeySet = new LinkedHashSet<>((List<String>) flexiblePivotDTO.get(ROW));

        pivotRowCount = setPivotHeader(exportHashMapData, headerMap, pivotRowCount, columnHeadingKeySet, rowHeadingKeySet);

        setPivotData(exportHashMapData, pivotRowCount, getResultHashMap, flexiblePivotDTO, headerMap, pivotColumnRowCount);

        return exportHashMapData;
    }

    // Local Method's
    // Header
    private int setPivotHeader(HashMap<Integer, HashMap<Integer, Object>> exportHashMapData, HashMap<String, String> headerMap, int pivotRowCount, Set<String> columnHeadingKeySet, Set<String> rowHeadingKeySet) {
        // Header COLUMN
        if (!columnHeadingKeySet.isEmpty()) {
            pivotRowCount = printColumnHeader(exportHashMapData, headerMap, pivotRowCount, columnHeadingKeySet);
        }

        // Header ROW
        if (!rowHeadingKeySet.isEmpty()) {
            pivotRowCount = printRowHeader(exportHashMapData, headerMap, pivotRowCount, rowHeadingKeySet);
        } else {
            // Only for Kpi
            pivotRowCount += 1;
        }

        return pivotRowCount;
    }

    // Main Data
    private void setPivotData(HashMap<Integer, HashMap<Integer, Object>> exportHashMapData, int pivotRowCount, List<HashMap<String, Object>> getResultHashMap, HashMap<String, Object> flexiblePivotDTO, HashMap<String, String> headerMap, int pivotColumnRowCount) {
        Set<String> rowHeadingKeySet = new LinkedHashSet<>((List<String>) flexiblePivotDTO.get("row"));
        Set<String> kpiHeadingKeySet = new LinkedHashSet<>((List<String>) flexiblePivotDTO.get("data"));
        Set<String> columnHeadingKeySet = new LinkedHashSet<>((List<String>) flexiblePivotDTO.get(COLUMN));

        int totalRowCount = rowHeadingKeySet.size();
        int totalKpiCount = this.getCountReturn(kpiHeadingKeySet, kpiHeadingKeySet.size()); // only for printing row and column when the kpi is zero

        int kpiColumnCount = this.getCountReturn(rowHeadingKeySet, totalRowCount);
        List<HashMap<String, Object>> columnTotalData = new ArrayList<>();
        HashMap<String, Object> rowTotalData = null;

        if (!columnHeadingKeySet.isEmpty()) {
            rowTotalData = getResultHashMap.get(getResultHashMap.size() - 1);
            getResultHashMap.remove(getResultHashMap.size() - 1);
        }

        HashMap<String, Integer> firstRowData = new HashMap<>();
        HashMap<String, Integer> rowRowTotalDataHashMap = new HashMap<>();

        int defaultRowDataCounter = pivotRowCount;
        int rowTotalColumnCount = kpiColumnCount + (totalKpiCount*getResultHashMap.size());

        int kpiColumnNumberForHeader = kpiColumnCount;
        int kpiLengthCounter = 0;
        List<Integer> kpiLengthList = new ArrayList<>();
        List<HashMap<String, Object>> rowDataListForGetResultHashMap = new ArrayList<>();
        List<HashMap<String, Object>> kpiDataListForGetResultHashMap = new ArrayList<>();
        List<HashMap<String, Object>> columnDataListForGetResultHashMap = new ArrayList<>();

        getDataIntoRowColumnKpiFromResponse(
            getResultHashMap, kpiLengthCounter, kpiLengthList, rowDataListForGetResultHashMap, kpiDataListForGetResultHashMap, columnDataListForGetResultHashMap
        );

        // Set Column Total Header
        if (!columnHeadingKeySet.isEmpty()) {
            List<HashMap<String, Object>> col = (List<HashMap<String, Object>>) rowTotalData.get("col");
            // Replace Tilde to Total
            columnDataListForGetResultHashMap.add(this.getColumnReplacedTildeWithTotal(col.get(0))); // adding

            for (HashMap<String, Object> columnData : columnDataListForGetResultHashMap) {
                // Header Data
                // Data Column Header
                printColumnDataForSingleResponse(exportHashMapData, columnData, columnHeadingKeySet, kpiColumnNumberForHeader, pivotColumnRowCount, totalKpiCount);
                // KPI Header
                printKpiHeaderFirSingleResponse(exportHashMapData, headerMap, kpiHeadingKeySet, kpiColumnNumberForHeader, defaultRowDataCounter);// SAME COUNT AS LAST COUNT OF printRowHeader FUNCTION
                kpiColumnNumberForHeader += totalKpiCount;
            }

            List<HashMap<String, Object>> rowRowTotalData = (List<HashMap<String, Object>>) rowTotalData.get("row");
            addRowRowTotalDataHashMap(rowRowTotalDataHashMap, rowRowTotalData);
        } else {
            printKpiHeaderFirSingleResponse(exportHashMapData, headerMap, kpiHeadingKeySet, kpiColumnNumberForHeader, defaultRowDataCounter);// SAME COUNT AS LAST COUNT OF printRowHeader FUNCTION
        }


        for (int rowNo = 0, rowDataListSize = rowDataListForGetResultHashMap.size(); rowNo < rowDataListSize; rowNo++) {
            HashMap<String, Object> rowData = rowDataListForGetResultHashMap.get(rowNo);
            HashMap<String, Object> kpiData = kpiDataListForGetResultHashMap.get(rowNo);

            if (isRowDataEmptyOrContainsValueTotal(rowData)) { //rowData.values().stream().allMatch(value -> "Total".equalsIgnoreCase((String) value))
                columnTotalData.add(kpiData);
                continue;
            }

            // Set Kpi Data
            String jsonKey = rowData.toString();
            if (firstRowData.get(jsonKey) != null) {
                int kpiRowNumber = firstRowData.get(jsonKey);
                printKPIData(exportHashMapData, kpiData, kpiRowNumber, kpiHeadingKeySet, kpiColumnCount);
            } else {
                // Header ROW
                pivotRowCount = printRowData(exportHashMapData, rowData, pivotRowCount, rowHeadingKeySet);
                printKPIData(exportHashMapData, kpiData, pivotRowCount, kpiHeadingKeySet, kpiColumnCount);
                printKPIRowTotalData(exportHashMapData, rowTotalData, rowData, kpiHeadingKeySet, pivotRowCount, rowTotalColumnCount, rowRowTotalDataHashMap);
                firstRowData.put(jsonKey, pivotRowCount);
            }

            if (kpiLengthList.contains(rowNo + 2)) {
                kpiColumnCount += totalKpiCount;
            }
        }

        // Data Column Row Total Print
        pivotRowCount++;

        if (!rowHeadingKeySet.isEmpty()) {
            setPivotDataColumnTotal(exportHashMapData, pivotRowCount, kpiHeadingKeySet, totalRowCount, columnTotalData);
        }
        if (isCornerTotal(rowHeadingKeySet, columnHeadingKeySet, rowTotalData)) {
            setPivotRowColumnCornerTotal(exportHashMapData, pivotRowCount, kpiHeadingKeySet, rowTotalData, rowTotalColumnCount);
        }
    }

    private void addRowRowTotalDataHashMap(HashMap<String, Integer> rowRowTotalDataHashMap, List<HashMap<String, Object>> rowRowTotalData) {
        for (int rowNo = 0, rowDataListSize = rowRowTotalData.size(); rowNo < rowDataListSize; rowNo++) {
            HashMap<String, Object> rowData = rowRowTotalData.get(rowNo);
            rowRowTotalDataHashMap.put(rowData.toString(), rowNo);
        }
    }

    private HashMap<String, Object> getColumnReplacedTildeWithTotal(HashMap<String, Object> columnDataRowTotalData) {
        for (Map.Entry<String, Object> entry : columnDataRowTotalData.entrySet()) {
            if (EIGHT_TILDE_SYMBOLS.equals(entry.getValue())) {
                columnDataRowTotalData.put(entry.getKey(), TOTAL);
            }
        }
        return columnDataRowTotalData;
    }

    private void getDataIntoRowColumnKpiFromResponse(List<HashMap<String, Object>> getResultHashMap, int kpiLengthCounter, List<Integer> kpiLengthList, List<HashMap<String, Object>> rowDataListForGetResultHashMap, List<HashMap<String, Object>> kpiDataListForGetResultHashMap, List<HashMap<String, Object>> columnDataListForGetResultHashMap) {
        for (HashMap<String, Object> result : getResultHashMap) {
            List<HashMap<String, Object>> kpi = (List<HashMap<String, Object>>) result.get("kpi");
            List<HashMap<String, Object>> row = (List<HashMap<String, Object>>) result.get("row");
            List<HashMap<String, Object>> column = (List<HashMap<String, Object>>) result.get("col");

            int kpiLength = kpi.size();
            kpiLengthCounter += kpiLength;
            kpiLengthList.add(kpiLengthCounter);

            for (int countKpi = 0; countKpi < kpiLength; countKpi++) {
                HashMap<String, Object> kpiData = kpi.get(countKpi);
                HashMap<String, Object> rowData = this.getRowColumnData(row, countKpi);
                HashMap<String, Object> columnData = this.getRowColumnData(column, countKpi);

                if (!columnDataListForGetResultHashMap.contains(columnData)) {
                    columnDataListForGetResultHashMap.add(columnData);
                }
                kpiDataListForGetResultHashMap.add(kpiData);
                rowDataListForGetResultHashMap.add(rowData);
            }
        }
    }

    private HashMap<String, Object> getRowColumnData(List<HashMap<String, Object>> rowColumn, int countKpi) {
        return (rowColumn != null && !rowColumn.isEmpty()) ? ((HashMap<String, Object>) rowColumn.get(countKpi)) : new HashMap<String, Object>();
    }

    private int getCountReturn(Set<String> keySet, int count) {
        return keySet.isEmpty() ? (1) : count;
    }

    private boolean isCornerTotal(Set<String> rowHeadingKeySet, Set<String> columnHeadingKeySet, HashMap<String, Object> rowTotalData) {
        return !rowHeadingKeySet.isEmpty() && !columnHeadingKeySet.isEmpty() && rowTotalData != null;
    }

    private boolean isRowDataEmptyOrContainsValueTotal(HashMap<String, Object> rowData) {
        return !rowData.isEmpty() && rowData.containsValue(TOTAL);
    }

    private void setPivotDataColumnTotal(HashMap<Integer, HashMap<Integer, Object>> exportHashMapData, int pivotRowCount, Set<String> kpiHeadingKeySet, int totalRowCount, List<HashMap<String, Object>> columnTotalData) {
        // PRINT TOTAL FOR COLUMN TOTAL
        for (int totalCount = 0; totalCount < totalRowCount; totalCount++) {
            putExcelExportDataMap(exportHashMapData, pivotRowCount, totalCount, TOTAL);
        }

        // PRINT TOTAL VALUE
        for (HashMap<String, Object> columnTotalResult : columnTotalData) {
            printKPIData(exportHashMapData, columnTotalResult, pivotRowCount, kpiHeadingKeySet, totalRowCount);
            totalRowCount += kpiHeadingKeySet.size();
        }
    }

    private void setPivotRowColumnCornerTotal(HashMap<Integer, HashMap<Integer, Object>> exportHashMapData, int pivotRowCount, Set<String> kpiHeadingKeySet, HashMap<String, Object> rowTotalData, int rowTotalColumnCount) {
        // FINAL CORNER TOTAL
        List<HashMap<String, Object>> row = (List<HashMap<String, Object>>) rowTotalData.get("row");
        List<HashMap<String, Object>> kpi = (List<HashMap<String, Object>>) rowTotalData.get("kpi");
        if (row.get(row.size() - 1).containsValue(EIGHT_TILDE_SYMBOLS)) {
            // SET CORNER TOTAL
            printKPIData(exportHashMapData, kpi.get(row.size() - 1), pivotRowCount, kpiHeadingKeySet, rowTotalColumnCount);
        } else {
            for (int cornerRowCount = 0; cornerRowCount < row.size(); cornerRowCount++) {
                HashMap<String, Object> finalCornerTotalRow = row.get(cornerRowCount);
                if (finalCornerTotalRow.containsValue(EIGHT_TILDE_SYMBOLS)) {
                    // SET CORNER TOTAL
                    printKPIData(exportHashMapData, kpi.get(cornerRowCount), pivotRowCount, kpiHeadingKeySet, rowTotalColumnCount);
                    break;
                }
            }
        }
    }

    // Base Method's
    // Header
    private int printColumnHeader(HashMap<Integer, HashMap<Integer, Object>> exportHashMapData, HashMap<String, String> headerMap, int rowCount, Set<String> columnHeadingKeySet) {
        for(String key : columnHeadingKeySet) {
            rowCount++;
            String header = headerMap.get(key);
            putExcelExportDataMap(exportHashMapData, rowCount, 0, (header == null || header.isEmpty()) ? (key) : header);
        }
        return rowCount;
    }

    private int printRowHeader(HashMap<Integer, HashMap<Integer, Object>> exportHashMapData, HashMap<String, String> headerMap, int rowCount, Set<String> rowHeadingKeySet) {
        rowCount++;
        int columnHeader = 0;
        for(String key : rowHeadingKeySet) {
            String header = headerMap.get(key);
            putExcelExportDataMap(exportHashMapData, rowCount, columnHeader, (header == null || header.isEmpty()) ? (key) : header);
            columnHeader++;
        }
        return rowCount;
    }

    // Header Data
    private void printColumnDataForSingleResponse(HashMap<Integer, HashMap<Integer, Object>> exportHashMapData, HashMap<String, Object> columnData, Set<String> columnHeadingKeySet, int columnPrintStartCell, int currentRowCount, int columnPrintEndCell) {
        if (!columnData.isEmpty()) {
            Object[] columnKeySet = columnHeadingKeySet.toArray();
            for (Object o : columnKeySet) {
                currentRowCount++;
                int end = columnPrintEndCell + columnPrintStartCell;
                for (int start = columnPrintStartCell; start < end; start++) {
                    String value = (String) columnData.get(o);
                    putExcelExportDataMap(exportHashMapData, currentRowCount, start, value);
                }
            }
        }
    }

    private void printKpiHeaderFirSingleResponse(HashMap<Integer, HashMap<Integer, Object>> exportHashMapData, HashMap<String, String> headerMap, Set<String> kpiHeadingKeySet, int columnPrintStartCell, int currentRowCount) {
        if (!kpiHeadingKeySet.isEmpty()) {
            Object[] columnKeySet = kpiHeadingKeySet.toArray();
            int kpiColumnLength = columnPrintStartCell + columnKeySet.length;
            for (int count = 0; columnPrintStartCell < kpiColumnLength; count++, columnPrintStartCell++) {
                String header = headerMap.get(columnKeySet[count]);

                putExcelExportDataMap(exportHashMapData, currentRowCount, columnPrintStartCell, (header == null || header.isEmpty()) ? (columnKeySet[count]) : header);
            }
        }
    }

    // Main Data
    private int printRowData(HashMap<Integer, HashMap<Integer, Object>> exportHashMapData, HashMap<String, Object> row, int rowCount, Set<String> rowHeadingKeySet) {
        if (!row.isEmpty()) {
            rowCount++;
            int columnHeader = 0;
            for(String key : rowHeadingKeySet) {
                Object value = /*row.get(key).equals(EIGHT_TILD_SYMBOLS) ? (TOTAL) :*/ row.get(key);
                putExcelExportDataMap(exportHashMapData, rowCount, columnHeader, value);
                columnHeader++;
            }
            return rowCount;
        } else {
            rowCount++;
            return rowCount;
        }
    }

    private void printKPIData(HashMap<Integer, HashMap<Integer, Object>> exportHashMapData, HashMap<String, Object> kpi, int rowCount, Set<String> kpiHeadingKeySet, int columnPrintStartCell) {
        int columnHeader = columnPrintStartCell;
        for(String key : kpiHeadingKeySet) {

            putExcelExportDataMap(exportHashMapData, rowCount, columnHeader, kpi.get(key));
            columnHeader++;
        }
    }

    private void printKPIRowTotalData(HashMap<Integer, HashMap<Integer, Object>> exportHashMapData, HashMap<String, Object> rowTotalData, HashMap<String, Object> rowData, Set<String> kpiHeadingKeySet, int pivotRowCount, int columnPrintStartCell, HashMap<String, Integer> rowRowTotalDataHashMap) {
        if (rowTotalData != null) {
            List<HashMap<String, Object>> kpiRowTotalData = (List<HashMap<String, Object>>) rowTotalData.get("kpi");
            List<HashMap<String, Object>> rowRowTotalData = (List<HashMap<String, Object>>) rowTotalData.get("row");

            if (rowRowTotalData == null) {
                for (HashMap<String, Object> kpiRowTotalDatum : kpiRowTotalData) {
                    printKPIData(exportHashMapData, kpiRowTotalDatum, pivotRowCount, kpiHeadingKeySet, columnPrintStartCell);
                }
                return;
            }

            String jsonKey = rowData.toString();
            if (rowRowTotalDataHashMap.get(jsonKey) != null) {
                printKPIData(exportHashMapData, kpiRowTotalData.get(rowRowTotalDataHashMap.get(jsonKey)), pivotRowCount, kpiHeadingKeySet, columnPrintStartCell);
            }
        }
    }

    private void putExcelExportDataMap(HashMap<Integer, HashMap<Integer, Object>> excelExportData, int row, int column, Object value) {
        HashMap<Integer, Object> integerObjectHashMap = null;
        if (excelExportData.containsKey(row)) {
            integerObjectHashMap = excelExportData.get(row);
        } else {
            integerObjectHashMap = new HashMap<>();
        }
        integerObjectHashMap.put(column, value);
        excelExportData.put(row, integerObjectHashMap);
    }
}
