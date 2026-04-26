package com.datagenerator.connection.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datagenerator.connection.api.ConnectionTestResponse;
import com.datagenerator.connection.api.TargetConnectionDraftTestRequest;
import com.datagenerator.connection.api.TargetConnectionUpsertRequest;
import com.datagenerator.connection.domain.DatabaseType;
import com.datagenerator.connection.domain.TargetConnection;
import com.datagenerator.connection.domain.TargetConnectionStatus;
import com.datagenerator.connection.repository.TargetConnectionRepository;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TargetConnectionServiceTest {

    @Mock
    private TargetConnectionRepository repository;

    @Mock
    private ConnectionProbeService connectionProbeService;

    @Mock
    private TableSchemaIntrospectionService tableSchemaIntrospectionService;

    @Mock
    private ConnectionJdbcSupport connectionJdbcSupport;

    @Mock
    private KafkaConnectionSupport kafkaConnectionSupport;

    private TargetConnectionService service;
    private TargetConnectionSecretCodec secretCodec;

    @BeforeEach
    void setUp() {
        secretCodec = new TargetConnectionSecretCodec("test-secret-key");
        service = new TargetConnectionService(
                repository,
                connectionProbeService,
                tableSchemaIntrospectionService,
                connectionJdbcSupport,
                kafkaConnectionSupport,
                secretCodec
        );
        lenient().when(repository.save(any(TargetConnection.class))).thenAnswer(invocation -> invocation.getArgument(0));
    }

    @Test
    void testDraft_shouldReuseExistingPasswordWhenEditingSavedConnection() {
        TargetConnection existing = new TargetConnection();
        existing.setId(8L);
        existing.setPasswordValue("existing-secret");
        when(repository.findById(8L)).thenReturn(Optional.of(existing));
        when(connectionJdbcSupport.normalizeParamsForStorage(eq(DatabaseType.MYSQL), any())).thenReturn("normalized");
        when(connectionProbeService.probe(any())).thenReturn(new ConnectionTestResponse(8L, true, "READY", "连接成功", "{}"));

        service.testDraft(new TargetConnectionDraftTestRequest(
                8L,
                new TargetConnectionUpsertRequest(
                        "订单库",
                        DatabaseType.MYSQL,
                        "127.0.0.1",
                        3306,
                        "synthetic_demo_target",
                        null,
                        "root",
                        null,
                        "useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai",
                        null,
                        TargetConnectionStatus.READY,
                        "测试"
                )
        ));

        ArgumentCaptor<TargetConnection> captor = ArgumentCaptor.forClass(TargetConnection.class);
        verify(connectionProbeService).probe(captor.capture());
        TargetConnection probed = captor.getValue();
        assertThat(probed.getId()).isEqualTo(8L);
        assertThat(probed.getPasswordValue()).isEqualTo("existing-secret");
        assertThat(probed.getHost()).isEqualTo("127.0.0.1");
        assertThat(probed.getJdbcParams()).isEqualTo("normalized");
    }

    @Test
    void create_shouldEncryptPasswordForStorage() {
        when(connectionJdbcSupport.normalizeParamsForStorage(eq(DatabaseType.MYSQL), any())).thenReturn("normalized");

        TargetConnection connection = service.create(new TargetConnectionUpsertRequest(
                "订单库",
                DatabaseType.MYSQL,
                "127.0.0.1",
                3306,
                "synthetic_demo_target",
                null,
                "root",
                "123456",
                "useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai",
                null,
                TargetConnectionStatus.READY,
                "测试"
        ));

        assertThat(connection.getPasswordValue()).startsWith("enc:v1:");
        assertThat(secretCodec.decryptForUse(connection.getPasswordValue())).isEqualTo("123456");
    }

    @Test
    void testDraft_shouldRequirePasswordForUnsavedConnection() {
        when(connectionJdbcSupport.normalizeParamsForStorage(eq(DatabaseType.MYSQL), any())).thenReturn("normalized");

        assertThatThrownBy(() -> service.testDraft(new TargetConnectionDraftTestRequest(
                null,
                new TargetConnectionUpsertRequest(
                        "订单库",
                        DatabaseType.MYSQL,
                        "127.0.0.1",
                        3306,
                        "synthetic_demo_target",
                        null,
                        "root",
                        null,
                        "useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai",
                        null,
                        TargetConnectionStatus.READY,
                        "测试"
                )
        )))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("password 不能为空");
    }

    @Test
    void create_shouldNormalizeKafkaConnectionAndAllowBlankPassword() {
        when(kafkaConnectionSupport.bootstrapServers(any(Map.class))).thenReturn("localhost:9092");
        when(kafkaConnectionSupport.firstEndpoint("localhost:9092"))
                .thenReturn(new KafkaConnectionSupport.BootstrapEndpoint("localhost", 9092));
        when(kafkaConnectionSupport.sanitizeConfig(any(Map.class))).thenAnswer(invocation -> invocation.getArgument(0));

        TargetConnection connection = service.create(new TargetConnectionUpsertRequest(
                "Kafka 目标",
                DatabaseType.KAFKA,
                null,
                9092,
                null,
                null,
                null,
                null,
                null,
                "{\"bootstrapServers\":\"localhost:9092\",\"securityProtocol\":\"PLAINTEXT\"}",
                TargetConnectionStatus.READY,
                "Kafka 测试"
        ));

        assertThat(connection.getHost()).isEqualTo("localhost");
        assertThat(connection.getPort()).isEqualTo(9092);
        assertThat(connection.getDatabaseName()).isEqualTo("kafka");
        assertThat(connection.getUsername()).isEmpty();
        assertThat(connection.getPasswordValue()).isEmpty();
        assertThat(connection.getConfigJson()).contains("bootstrapServers");
    }
}
