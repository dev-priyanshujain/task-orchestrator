package com.orchestrator.scheduler.controller;

import com.orchestrator.common.dto.TaskRequest;
import com.orchestrator.common.dto.TaskResponse;
import com.orchestrator.common.model.TaskStatus;
import com.orchestrator.scheduler.service.TaskService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

/**
 * REST API for task management.
 *
 * POST   /tasks          — Create a new task
 * GET    /tasks/{id}     — Get task status and metadata
 * DELETE /tasks/{id}     — Cancel a pending task
 * GET    /tasks?status=  — List tasks filtered by status
 */
@RestController
@RequestMapping("/tasks")
@RequiredArgsConstructor
public class TaskController {

    private final TaskService taskService;

    @PostMapping
    public ResponseEntity<TaskResponse> createTask(@Valid @RequestBody TaskRequest request) {
        TaskResponse response = taskService.createTask(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @GetMapping("/{id}")
    public ResponseEntity<TaskResponse> getTask(@PathVariable UUID id) {
        return ResponseEntity.ok(taskService.getTask(id));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> cancelTask(@PathVariable UUID id) {
        taskService.cancelTask(id);
        return ResponseEntity.noContent().build();
    }

    @GetMapping
    public ResponseEntity<List<TaskResponse>> listTasks(@RequestParam(required = false) TaskStatus status) {
        if (status == null) {
            return ResponseEntity.ok(taskService.getAllTasks());
        }
        return ResponseEntity.ok(taskService.getTasksByStatus(status));
    }
}
