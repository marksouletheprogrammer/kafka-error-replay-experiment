package com.example.connect.replay.repository;

import com.example.connect.replay.model.ErrorRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface ErrorRecordRepository extends JpaRepository<ErrorRecord, UUID> {

    List<ErrorRecord> findByStatus(String status);

    List<ErrorRecord> findByConnectorName(String connectorName);

    List<ErrorRecord> findByOriginalTopic(String originalTopic);

    List<ErrorRecord> findByStatusAndConnectorName(String status, String connectorName);

    List<ErrorRecord> findByStatusAndOriginalTopic(String status, String originalTopic);

    @Query("SELECT e.status, COUNT(e) FROM ErrorRecord e GROUP BY e.status")
    List<Object[]> countByStatus();

    @Query("SELECT e.errorCode, COUNT(e) FROM ErrorRecord e GROUP BY e.errorCode")
    List<Object[]> countByErrorCode();

    @Query("SELECT e.connectorName, COUNT(e) FROM ErrorRecord e GROUP BY e.connectorName")
    List<Object[]> countByConnector();

    @Query("SELECT e.connectorName, e.status, COUNT(e) FROM ErrorRecord e GROUP BY e.connectorName, e.status")
    List<Object[]> countByConnectorAndStatus();
}
