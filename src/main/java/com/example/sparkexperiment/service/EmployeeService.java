package com.example.sparkexperiment.service;

import com.example.sparkexperiment.modal.Employee;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Service
public class EmployeeService {

    public List<Map<String,Object>> addEmployee(Employee employee) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("EmployeeManagement")
                .master("local[*]")
                .getOrCreate();

        try {
            List<Row> employeedf = Arrays.asList(
                    RowFactory.create(
                            employee.getId(),
                            employee.getEmail(),
                            employee.getName(),
                            employee.getPassword(),
                            employee.getMobile(),
                            employee.getSalary()
                    )
            );

            Dataset<Row> df = sparkSession.createDataFrame(employeedf,Employee.class);

            Encoder<Map<String, Object>> mapEncoder = Encoders.kryo((Class<Map<String, Object>>) (Class<?>) Map.class);

            Dataset<Map<String, Object>> mappedDF = df.map((MapFunction<Row, Map<String, Object>>) this::rowToMap, mapEncoder);

            return mappedDF.collectAsList();
        } finally {
            sparkSession.stop();
        }
    }

    private Map<String, Object> rowToMap(Row row) {
        Map<String, Object> resultMap = new java.util.HashMap<>();
        resultMap.put("id", row.getLong(0));
        resultMap.put("email", row.getString(1) + " (Mapped)");
        resultMap.put("name", row.getString(2));
        resultMap.put("password", row.getString(3));
        resultMap.put("mobile", row.getLong(4));
        resultMap.put("salary", row.getDouble(5));
        return resultMap;
    }
}
