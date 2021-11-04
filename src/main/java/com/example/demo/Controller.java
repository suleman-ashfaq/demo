package com.example.demo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.UUID;


@RestController
public class Controller {

    private static final Logger LOG = LoggerFactory.getLogger(Controller.class);
    ObjectMapper om = new ObjectMapper();
    private RestTemplate restTemplate = new RestTemplate();

    @Value("${server.port}")
    private String serverPort;

    private CuratorLeader curatorLeader;

    @PostConstruct
    public void init() {
        UUID uuid = UUID.randomUUID();
        try{
            curatorLeader = new CuratorLeader(uuid.toString().substring(0,5), "2181",
                    new ExponentialBackoffRetry(1000, 5));
            curatorLeader.startZK();
            curatorLeader.setServerPort(serverPort);
            curatorLeader.register();
            curatorLeader.runForMaster();

        } catch (Exception e) {
            LOG.error("Failed to setup instance");
        }

    }

    @RequestMapping("/")
    public ResponseEntity<String> index() {
        try {
            String leaderPort = curatorLeader.getLeaderPort();
            if (curatorLeader.isLeader()) {
                return ResponseEntity.ok("Running Leader on port " + leaderPort);
            } else
                return ResponseEntity.ok("Running Follower on port " + serverPort);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return ResponseEntity.ok("Running Server...");
    }

    @RequestMapping("/students")
    public ResponseEntity<String> getStudents() throws JsonProcessingException {
        List<Student> students = VolatileStorage.getStudentList();
        String value = om.writeValueAsString(students);
        return ResponseEntity.ok().body(value);
    }

    @PostMapping("/student/{id}/{name}")
    public ResponseEntity<String> addStudent(@PathVariable int id, @PathVariable String name, @RequestHeader(name = "request_from", required = false) String requestFrom) {
        LOG.info("POST request from: {}, id: {}, name: {} ", requestFrom, id, name);
        if (!curatorLeader.isLeader()) {
            try {
                String leaderPort = curatorLeader.getLeaderPort();
                LOG.info("leader: {}, from: {}, equality: {}", leaderPort, requestFrom, leaderPort.equals(requestFrom));
                if (leaderPort.equals(requestFrom)) //meaning its already fwd request from leader
                    VolatileStorage.addStudent(new Student(id, name));
                else
                    fwdRequestMsg(serverPort, leaderPort, id, name);
            } catch (Exception e) {
                System.out.println("Cannot route to leader");
                e.printStackTrace();
            }
            return ResponseEntity.ok("I am just a follower, re-routing request");
        } else {
            VolatileStorage.addStudent(new Student(id, name));
            List<String> ports = curatorLeader.getAllPorts();
            for (String port : ports) {
                if (port.equals(serverPort)) //skip leader's own port (serverPort here == leaderPort)
                    continue;
                LOG.info("Sending data to follower: {}", port);
                fwdRequestMsg(serverPort, port, id, name);
            }
            return ResponseEntity.ok("Added successfully and sending to all ports: "+ ports);
        }
    }

    private void fwdRequestMsg(String fromPort, String toPort, int id, String name) {
        String requestUrl =
                "http://"
                        .concat("localhost:")
                        .concat(toPort)
                        .concat("/")
                        .concat("student")
                        .concat("/")
                        .concat(String.valueOf(id))
                        .concat("/")
                        .concat(name);
        HttpHeaders headers = new HttpHeaders();
        headers.add("request_from", fromPort);
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<String> entity = new HttpEntity<>(headers);
        restTemplate.exchange(requestUrl, HttpMethod.POST, entity, String.class).getBody();
    }


}
