package com.example.sparkexperiment.controller;

import com.example.sparkexperiment.modal.Employee;
import com.example.sparkexperiment.service.EmployeeService;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class EmployeeController {
    @Autowired
    private EmployeeService employeeService;

    @PostMapping("/addEmployee")
    public List<Row> addEmployee(@RequestBody Employee employee){
        return employeeService.addEmployee(employee);
    }
}
