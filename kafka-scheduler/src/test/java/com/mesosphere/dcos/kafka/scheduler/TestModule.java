package com.mesosphere.dcos.kafka.scheduler;

import com.google.inject.AbstractModule;
import com.mesosphere.dcos.kafka.config.KafkaSchedulerConfiguration;

/**
 * A module for dependency injection when running unit tests.
 */
public class TestModule extends AbstractModule {
    private final KafkaSchedulerConfiguration configuration;

    public TestModule(final KafkaSchedulerConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    protected void configure() {
        bind(KafkaSchedulerConfiguration.class).toInstance(this.configuration);
    }
}
