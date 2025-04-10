/*
 * Copyright 2018-present HiveMQ GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hivemq.extensions.session;

import com.hivemq.extension.sdk.api.ExtensionMain;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.events.EventRegistry;
import com.hivemq.extension.sdk.api.parameter.*;
import com.hivemq.extension.sdk.api.services.Services;
import com.hivemq.extension.sdk.api.services.exception.IterationFailedException;
import com.hivemq.extension.sdk.api.services.exception.NoSuchClientIdException;
import com.hivemq.extension.sdk.api.services.general.IterationCallback;
import com.hivemq.extension.sdk.api.services.general.IterationContext;
import com.hivemq.extension.sdk.api.services.intializer.InitializerRegistry;
import com.hivemq.extension.sdk.api.services.session.ClientService;
import com.hivemq.extension.sdk.api.services.session.SessionInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

/**
 * This is the main class of the extension,
 * which is instantiated either during the HiveMQ start up process (if extension is enabled)
 * or when HiveMQ is already started by enabling the extension.
 * Once it starts, it will iterate all clients one by one.
 *
 * @author Dasha Samkova
 * @since 4.38.0
 */
public class HelloWorldMain implements ExtensionMain {

    private static final @NotNull Logger log = LoggerFactory.getLogger(HelloWorldMain.class);

    @Override
    public void extensionStart(
            final @NotNull ExtensionStartInput extensionStartInput,
            final @NotNull ExtensionStartOutput extensionStartOutput) {

        try {
            final ExtensionInformation extensionInformation = extensionStartInput.getExtensionInformation();
            log.info("Started " + extensionInformation.getName() + ":" + extensionInformation.getVersion());

            iterate(0);

        } catch (final Exception e) {
            log.error("Exception thrown at extension start: ", e);
        }
    }

    @Override
    public void extensionStop(
            final @NotNull ExtensionStopInput extensionStopInput,
            final @NotNull ExtensionStopOutput extensionStopOutput) {

        final ExtensionInformation extensionInformation = extensionStopInput.getExtensionInformation();
        log.info("Stopped " + extensionInformation.getName() + ":" + extensionInformation.getVersion());
    }

    public void iterate(final int attempts) {

        final AtomicInteger counter = new AtomicInteger();

        CompletableFuture<Void> iterationFuture = Services.clientService().iterateAllClients(
                new IterationCallback<SessionInformation>() {
                    @Override
                    public void iterate(IterationContext context, SessionInformation sessionInformation) {
                        if (sessionInformation.isConnected()) {
                            counter.incrementAndGet();
                        } else {

                            System.out.println(">>Client ID: " +  sessionInformation.getClientIdentifier() + ", Session Expiry Interval: " +
                            sessionInformation.getSessionExpiryInterval());

                            if ( 3*24*60*60 < sessionInformation.getSessionExpiryInterval()) {
                                final ClientService clientService = Services.clientService();
                                final String clientId = sessionInformation.getClientIdentifier();
                                CompletableFuture<Boolean> invalidateSessionFuture = clientService.invalidateSession(clientId);

                                invalidateSessionFuture.whenComplete(new BiConsumer<Boolean, Throwable>() {
                                    @Override
                                    public void accept(Boolean disconnected, Throwable throwable) {
                                        if(throwable == null) {
                                            if(disconnected){
                                                log.info(">>Client was disconnected" + " " + clientId);
                                            } else {
                                                log.info(">>Client is already disconnected" + " " + clientId);
                                            }
                                            log.info(">>Client session was removed" + " " + clientId);

                                            // Sleep 1 second after session invalidation
                                            try {
                                                Thread.sleep(1000);
                                            } catch (InterruptedException e) {
                                                Thread.currentThread().interrupt();
                                                log.warn("Sleep interrupted after session removal", e);
                                            }

                                        } else {
                                            if(throwable instanceof NoSuchClientIdException){
                                                log.info(">>Client not found" + " " + clientId);
                                            }
                                            //please use more sophisticated logging
                                            throwable.printStackTrace();
                                        }
                                    }
                                });
                            }
                        }
                    }
                });

        iterationFuture.whenComplete((ignored, throwable) -> {
            if (throwable == null) {
                System.out.println("Connected clients: " + counter.get());

                // in case the cluster topology changes during iteration, an IterationFailedException is thrown
            } else if (throwable instanceof IterationFailedException) {
                // only retry 3 times
                if (attempts < 3) {
                    final int newAttemptCount = attempts + 1;
                    Services.extensionExecutorService().schedule(() ->
                            iterate(newAttemptCount), newAttemptCount * 10, TimeUnit.SECONDS); // schedule retry with delay in case topology change is not over, else we would get another IterationFailedException
                } else {
                    System.out.println("Could not fully iterate all clients.");
                }
            } else {
                throwable.printStackTrace(); // please use more sophisticated logging
            }
        });
    }


}