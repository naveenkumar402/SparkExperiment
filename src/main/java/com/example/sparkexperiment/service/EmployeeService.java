package com.example.sparkexperiment.service;

import com.example.sparkexperiment.modal.Employee;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

@Service
public class EmployeeService {

    public List<Row> addEmployee(Employee employee){

        SparkSession sparkSession=SparkSession.builder()
                .appName("EmployeeManagement")
                .master("local[*]")
                .getOrCreate();
        try{
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
            Dataset<Row> df=sparkSession.createDataFrame(employeedf,Employee.class);

            Dataset<Row> mappedDF=df.map((MapFunction<Row, Row>) this::rowFunction, Encoders.bean(Row.class));
            return mappedDF.collectAsList();
        }
        finally {
            sparkSession.stop();
        }

    }
    private Row rowFunction(Row row) {
        String updatedName = row.getString(2);
        String email = row.getString(1)+ " (Mapped)";
        return RowFactory.create(row.getLong(0),email,updatedName,row.getString(3),row.getLong(4),row.getDouble(5));
    }


}