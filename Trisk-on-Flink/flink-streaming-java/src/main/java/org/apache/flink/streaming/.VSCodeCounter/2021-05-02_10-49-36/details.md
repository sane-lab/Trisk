# Details

Date : 2021-05-02 10:49:36

Directory /home/myc/workspace/flink-related/flink/flink-streaming-java/src/main/java/org/apache/flink/streaming/controlplane

Total : 101 files,  9048 codes, 2637 comments, 2292 blanks, all 13977 lines

[summary](results.md)

## Files
| filename | language | code | comment | blank | total |
| :--- | :--- | ---: | ---: | ---: | ---: |
| [controlplane/dispatcher/DefaultStreamManagerRunnerFactory.java](/controlplane/dispatcher/DefaultStreamManagerRunnerFactory.java) | Java | 50 | 22 | 10 | 82 |
| [controlplane/dispatcher/JobStreamManagerDispatcherFactory.java](/controlplane/dispatcher/JobStreamManagerDispatcherFactory.java) | Java | 25 | 28 | 9 | 62 |
| [controlplane/dispatcher/MiniStreamManagerDispatcher.java](/controlplane/dispatcher/MiniStreamManagerDispatcher.java) | Java | 55 | 26 | 14 | 95 |
| [controlplane/dispatcher/PartialStreamManagerDispatcherServices.java](/controlplane/dispatcher/PartialStreamManagerDispatcherServices.java) | Java | 52 | 21 | 16 | 89 |
| [controlplane/dispatcher/PartialStreamManagerDispatcherServicesWithJobGraphStore.java](/controlplane/dispatcher/PartialStreamManagerDispatcherServicesWithJobGraphStore.java) | Java | 41 | 20 | 9 | 70 |
| [controlplane/dispatcher/SessionStreamManagerDispatcherFactory.java](/controlplane/dispatcher/SessionStreamManagerDispatcherFactory.java) | Java | 23 | 21 | 6 | 50 |
| [controlplane/dispatcher/StandaloneStreamManagerDispatcher.java](/controlplane/dispatcher/StandaloneStreamManagerDispatcher.java) | Java | 24 | 22 | 5 | 51 |
| [controlplane/dispatcher/StreamManagerDispatcher.java](/controlplane/dispatcher/StreamManagerDispatcher.java) | Java | 391 | 63 | 109 | 563 |
| [controlplane/dispatcher/StreamManagerDispatcherFactory.java](/controlplane/dispatcher/StreamManagerDispatcherFactory.java) | Java | 21 | 23 | 8 | 52 |
| [controlplane/dispatcher/StreamManagerDispatcherGateway.java](/controlplane/dispatcher/StreamManagerDispatcherGateway.java) | Java | 19 | 39 | 8 | 66 |
| [controlplane/dispatcher/StreamManagerDispatcherId.java](/controlplane/dispatcher/StreamManagerDispatcherId.java) | Java | 19 | 27 | 11 | 57 |
| [controlplane/dispatcher/StreamManagerDispatcherRestEndpoint.java](/controlplane/dispatcher/StreamManagerDispatcherRestEndpoint.java) | Java | 77 | 46 | 21 | 144 |
| [controlplane/dispatcher/StreamManagerDispatcherServices.java](/controlplane/dispatcher/StreamManagerDispatcherServices.java) | Java | 81 | 20 | 21 | 122 |
| [controlplane/dispatcher/StreamManagerRunnerFactory.java](/controlplane/dispatcher/StreamManagerRunnerFactory.java) | Java | 22 | 21 | 5 | 48 |
| [controlplane/dispatcher/runner/AbstractStreamManagerDispatcherLeaderProcess.java](/controlplane/dispatcher/runner/AbstractStreamManagerDispatcherLeaderProcess.java) | Java | 179 | 20 | 54 | 253 |
| [controlplane/dispatcher/runner/DefaultStreamManagerDispatcherGatewayService.java](/controlplane/dispatcher/runner/DefaultStreamManagerDispatcherGatewayService.java) | Java | 33 | 17 | 12 | 62 |
| [controlplane/dispatcher/runner/DefaultStreamManagerDispatcherGatewayServiceFactory.java](/controlplane/dispatcher/runner/DefaultStreamManagerDispatcherGatewayServiceFactory.java) | Java | 42 | 20 | 12 | 74 |
| [controlplane/dispatcher/runner/DefaultStreamManagerDispatcherRunner.java](/controlplane/dispatcher/runner/DefaultStreamManagerDispatcherRunner.java) | Java | 134 | 25 | 38 | 197 |
| [controlplane/dispatcher/runner/DefaultStreamManagerDispatcherRunnerFactory.java](/controlplane/dispatcher/runner/DefaultStreamManagerDispatcherRunnerFactory.java) | Java | 46 | 21 | 11 | 78 |
| [controlplane/dispatcher/runner/JobStreamManagerDispatcherLeaderProcess.java](/controlplane/dispatcher/runner/JobStreamManagerDispatcherLeaderProcess.java) | Java | 28 | 20 | 10 | 58 |
| [controlplane/dispatcher/runner/JobStreamManagerDispatcherLeaderProcessFactory.java](/controlplane/dispatcher/runner/JobStreamManagerDispatcherLeaderProcessFactory.java) | Java | 22 | 20 | 9 | 51 |
| [controlplane/dispatcher/runner/JobStreamManagerDispatcherLeaderProcessFactoryFactory.java](/controlplane/dispatcher/runner/JobStreamManagerDispatcherLeaderProcessFactoryFactory.java) | Java | 46 | 20 | 13 | 79 |
| [controlplane/dispatcher/runner/SessionStreamManagerDispatcherLeaderProcess.java](/controlplane/dispatcher/runner/SessionStreamManagerDispatcherLeaderProcess.java) | Java | 196 | 28 | 48 | 272 |
| [controlplane/dispatcher/runner/SessionStreamManagerDispatcherLeaderProcessFactory.java](/controlplane/dispatcher/runner/SessionStreamManagerDispatcherLeaderProcessFactory.java) | Java | 31 | 20 | 8 | 59 |
| [controlplane/dispatcher/runner/SessionStreamManagerDispatcherLeaderProcessFactoryFactory.java](/controlplane/dispatcher/runner/SessionStreamManagerDispatcherLeaderProcessFactoryFactory.java) | Java | 36 | 20 | 10 | 66 |
| [controlplane/dispatcher/runner/StoppedStreamManagerDispatcherLeaderProcess.java](/controlplane/dispatcher/runner/StoppedStreamManagerDispatcherLeaderProcess.java) | Java | 34 | 21 | 12 | 67 |
| [controlplane/dispatcher/runner/StreamManagerDispatcherLeaderProcess.java](/controlplane/dispatcher/runner/StreamManagerDispatcherLeaderProcess.java) | Java | 14 | 20 | 10 | 44 |
| [controlplane/dispatcher/runner/StreamManagerDispatcherLeaderProcessFactory.java](/controlplane/dispatcher/runner/StreamManagerDispatcherLeaderProcessFactory.java) | Java | 5 | 20 | 5 | 30 |
| [controlplane/dispatcher/runner/StreamManagerDispatcherLeaderProcessFactoryFactory.java](/controlplane/dispatcher/runner/StreamManagerDispatcherLeaderProcessFactoryFactory.java) | Java | 17 | 20 | 6 | 43 |
| [controlplane/dispatcher/runner/StreamManagerDispatcherRunner.java](/controlplane/dispatcher/runner/StreamManagerDispatcherRunner.java) | Java | 8 | 27 | 6 | 41 |
| [controlplane/dispatcher/runner/StreamManagerDispatcherRunnerFactory.java](/controlplane/dispatcher/runner/StreamManagerDispatcherRunnerFactory.java) | Java | 18 | 20 | 6 | 44 |
| [controlplane/dispatcher/runner/StreamManagerDispatcherRunnerLeaderElectionLifecycleManager.java](/controlplane/dispatcher/runner/StreamManagerDispatcherRunnerLeaderElectionLifecycleManager.java) | Java | 37 | 17 | 13 | 67 |
| [controlplane/entrypoint/ClusterControllerEntrypoint.java](/controlplane/entrypoint/ClusterControllerEntrypoint.java) | Java | 390 | 67 | 112 | 569 |
| [controlplane/entrypoint/SessionClusterControllerEntrypoint.java](/controlplane/entrypoint/SessionClusterControllerEntrypoint.java) | Java | 33 | 20 | 9 | 62 |
| [controlplane/entrypoint/SessionStreamManagerEntrypoint.java](/controlplane/entrypoint/SessionStreamManagerEntrypoint.java) | Java | 32 | 20 | 9 | 61 |
| [controlplane/entrypoint/StandaloneSessionClusterControllerEntrypoint.java](/controlplane/entrypoint/StandaloneSessionClusterControllerEntrypoint.java) | Java | 40 | 21 | 13 | 74 |
| [controlplane/entrypoint/StandaloneSessionStreamManagerEntrypoint.java](/controlplane/entrypoint/StandaloneSessionStreamManagerEntrypoint.java) | Java | 37 | 21 | 12 | 70 |
| [controlplane/entrypoint/StreamManagerEntrypoint.java](/controlplane/entrypoint/StreamManagerEntrypoint.java) | Java | 327 | 66 | 97 | 490 |
| [controlplane/entrypoint/component/DefaultStreamManagerDispatcherComponentFactory.java](/controlplane/entrypoint/component/DefaultStreamManagerDispatcherComponentFactory.java) | Java | 137 | 21 | 30 | 188 |
| [controlplane/entrypoint/component/StreamManagerDispatcherComponent.java](/controlplane/entrypoint/component/StreamManagerDispatcherComponent.java) | Java | 85 | 30 | 29 | 144 |
| [controlplane/entrypoint/component/StreamManagerDispatcherComponentFactory.java](/controlplane/entrypoint/component/StreamManagerDispatcherComponentFactory.java) | Java | 19 | 20 | 6 | 45 |
| [controlplane/jobgraph/DefaultExecutionPlanAndJobGraphUpdaterFactory.java](/controlplane/jobgraph/DefaultExecutionPlanAndJobGraphUpdaterFactory.java) | Java | 17 | 0 | 5 | 22 |
| [controlplane/jobgraph/ExecutionPlanBuilder.java](/controlplane/jobgraph/ExecutionPlanBuilder.java) | Java | 180 | 26 | 17 | 223 |
| [controlplane/jobgraph/StreamJobGraphUpdater.java](/controlplane/jobgraph/StreamJobGraphUpdater.java) | Java | 116 | 9 | 17 | 142 |
| [controlplane/reconfigure/ControlFunctionManager.java](/controlplane/reconfigure/ControlFunctionManager.java) | Java | 31 | 9 | 11 | 51 |
| [controlplane/reconfigure/ControlFunctionManagerService.java](/controlplane/reconfigure/ControlFunctionManagerService.java) | Java | 7 | 0 | 6 | 13 |
| [controlplane/reconfigure/TestingCFManager.java](/controlplane/reconfigure/TestingCFManager.java) | Java | 70 | 0 | 19 | 89 |
| [controlplane/reconfigure/operator/ControlContext.java](/controlplane/reconfigure/operator/ControlContext.java) | Java | 12 | 0 | 5 | 17 |
| [controlplane/reconfigure/operator/ControlFunction.java](/controlplane/reconfigure/operator/ControlFunction.java) | Java | 6 | 45 | 5 | 56 |
| [controlplane/reconfigure/operator/ControlOperator.java](/controlplane/reconfigure/operator/ControlOperator.java) | Java | 27 | 0 | 7 | 34 |
| [controlplane/reconfigure/operator/ControlOperatorFactory.java](/controlplane/reconfigure/operator/ControlOperatorFactory.java) | Java | 47 | 5 | 14 | 66 |
| [controlplane/reconfigure/operator/OperatorIOTypeDescriptor.java](/controlplane/reconfigure/operator/OperatorIOTypeDescriptor.java) | Java | 8 | 4 | 6 | 18 |
| [controlplane/reconfigure/type/FunctionLoader.java](/controlplane/reconfigure/type/FunctionLoader.java) | Java | 3 | 0 | 2 | 5 |
| [controlplane/reconfigure/type/FunctionTypeStorage.java](/controlplane/reconfigure/type/FunctionTypeStorage.java) | Java | 6 | 4 | 6 | 16 |
| [controlplane/reconfigure/type/InMemoryFunctionStorge.java](/controlplane/reconfigure/type/InMemoryFunctionStorge.java) | Java | 11 | 0 | 5 | 16 |
| [controlplane/rescale/RescaleActionConsumer.java](/controlplane/rescale/RescaleActionConsumer.java) | Java | 76 | 8 | 20 | 104 |
| [controlplane/rescale/StreamJobGraphRescaler.java](/controlplane/rescale/StreamJobGraphRescaler.java) | Java | 139 | 21 | 39 | 199 |
| [controlplane/rescale/controller/OperatorController.java](/controlplane/rescale/controller/OperatorController.java) | Java | 9 | 1 | 7 | 17 |
| [controlplane/rescale/controller/OperatorControllerListener.java](/controlplane/rescale/controller/OperatorControllerListener.java) | Java | 8 | 0 | 6 | 14 |
| [controlplane/rescale/metrics/KafkaMetricsRetriever.java](/controlplane/rescale/metrics/KafkaMetricsRetriever.java) | Java | 208 | 29 | 42 | 279 |
| [controlplane/rescale/metrics/StockMetricsRetriever.java](/controlplane/rescale/metrics/StockMetricsRetriever.java) | Java | 234 | 31 | 47 | 312 |
| [controlplane/rescale/metrics/StreamSwitchMetricsRetriever.java](/controlplane/rescale/metrics/StreamSwitchMetricsRetriever.java) | Java | 10 | 1 | 7 | 18 |
| [controlplane/rescale/streamswitch/ConfigurableDummyStreamSwitch.java](/controlplane/rescale/streamswitch/ConfigurableDummyStreamSwitch.java) | Java | 173 | 69 | 47 | 289 |
| [controlplane/rescale/streamswitch/DummyStreamSwitch.java](/controlplane/rescale/streamswitch/DummyStreamSwitch.java) | Java | 314 | 93 | 60 | 467 |
| [controlplane/rescale/streamswitch/FlinkOperatorController.java](/controlplane/rescale/streamswitch/FlinkOperatorController.java) | Java | 9 | 0 | 5 | 14 |
| [controlplane/rescale/streamswitch/FlinkStreamSwitchAdaptor.java](/controlplane/rescale/streamswitch/FlinkStreamSwitchAdaptor.java) | Java | 154 | 15 | 54 | 223 |
| [controlplane/rescale/streamswitch/Pair.java](/controlplane/rescale/streamswitch/Pair.java) | Java | 31 | 0 | 11 | 42 |
| [controlplane/rescale/streamswitch/RescaleActionDescriptor.java](/controlplane/rescale/streamswitch/RescaleActionDescriptor.java) | Java | 108 | 16 | 28 | 152 |
| [controlplane/rescale/streamswitch/StreamSwitch.java](/controlplane/rescale/streamswitch/StreamSwitch.java) | Java | 141 | 36 | 42 | 219 |
| [controlplane/rest/JobStreamManagerRestEndpointFactory.java](/controlplane/rest/JobStreamManagerRestEndpointFactory.java) | Java | 37 | 20 | 7 | 64 |
| [controlplane/rest/SessionStreamManagerRestEndpointFactory.java](/controlplane/rest/SessionStreamManagerRestEndpointFactory.java) | Java | 36 | 20 | 7 | 63 |
| [controlplane/rest/StreamManagerRestEndpointFactory.java](/controlplane/rest/StreamManagerRestEndpointFactory.java) | Java | 28 | 22 | 7 | 57 |
| [controlplane/rest/StreamManagerRestServerEndpointConfiguration.java](/controlplane/rest/StreamManagerRestServerEndpointConfiguration.java) | Java | 101 | 57 | 31 | 189 |
| [controlplane/rest/handler/AbstractStreamManagerHandler.java](/controlplane/rest/handler/AbstractStreamManagerHandler.java) | Java | 194 | 51 | 30 | 275 |
| [controlplane/rest/handler/AbstractStreamManagerRestHandler.java](/controlplane/rest/handler/AbstractStreamManagerRestHandler.java) | Java | 46 | 40 | 12 | 98 |
| [controlplane/rest/handler/InFlightRequestTracker.java](/controlplane/rest/handler/InFlightRequestTracker.java) | Java | 26 | 32 | 10 | 68 |
| [controlplane/rest/handler/StreamManagerLeaderRetrievalHandler.java](/controlplane/rest/handler/StreamManagerLeaderRetrievalHandler.java) | Java | 63 | 23 | 15 | 101 |
| [controlplane/rest/handler/job/StreamManagerJobExecutionResultHandler.java](/controlplane/rest/handler/job/StreamManagerJobExecutionResultHandler.java) | Java | 67 | 20 | 12 | 99 |
| [controlplane/rest/handler/job/StreamManagerJobSubmitHandler.java](/controlplane/rest/handler/job/StreamManagerJobSubmitHandler.java) | Java | 145 | 20 | 28 | 193 |
| [controlplane/streammanager/StreamManager.java](/controlplane/streammanager/StreamManager.java) | Java | 940 | 189 | 135 | 1,264 |
| [controlplane/streammanager/StreamManagerConfiguration.java](/controlplane/streammanager/StreamManagerConfiguration.java) | Java | 54 | 22 | 21 | 97 |
| [controlplane/streammanager/StreamManagerMiniDispatcherRestEndpoint.java](/controlplane/streammanager/StreamManagerMiniDispatcherRestEndpoint.java) | Java | 35 | 20 | 6 | 61 |
| [controlplane/streammanager/StreamManagerRunner.java](/controlplane/streammanager/StreamManagerRunner.java) | Java | 10 | 36 | 8 | 54 |
| [controlplane/streammanager/StreamManagerRunnerImpl.java](/controlplane/streammanager/StreamManagerRunnerImpl.java) | Java | 241 | 65 | 71 | 377 |
| [controlplane/streammanager/StreamManagerRuntimeServices.java](/controlplane/streammanager/StreamManagerRuntimeServices.java) | Java | 24 | 17 | 9 | 50 |
| [controlplane/streammanager/StreamManagerService.java](/controlplane/streammanager/StreamManagerService.java) | Java | 12 | 45 | 9 | 66 |
| [controlplane/streammanager/abstraction/ExecutionPlanImpl.java](/controlplane/streammanager/abstraction/ExecutionPlanImpl.java) | Java | 186 | 47 | 35 | 268 |
| [controlplane/streammanager/abstraction/ExecutionPlanWithLock.java](/controlplane/streammanager/abstraction/ExecutionPlanWithLock.java) | Java | 83 | 54 | 26 | 163 |
| [controlplane/streammanager/abstraction/ReconfigurationExecutor.java](/controlplane/streammanager/abstraction/ReconfigurationExecutor.java) | Java | 27 | 35 | 19 | 81 |
| [controlplane/streammanager/abstraction/Transformations.java](/controlplane/streammanager/abstraction/Transformations.java) | Java | 21 | 1 | 8 | 30 |
| [controlplane/streammanager/exceptions/StreamManagerException.java](/controlplane/streammanager/exceptions/StreamManagerException.java) | Java | 14 | 17 | 7 | 38 |
| [controlplane/streammanager/factories/DefaultStreamManagerServiceFactory.java](/controlplane/streammanager/factories/DefaultStreamManagerServiceFactory.java) | Java | 50 | 24 | 14 | 88 |
| [controlplane/streammanager/factories/StreamManagerServiceFactory.java](/controlplane/streammanager/factories/StreamManagerServiceFactory.java) | Java | 8 | 20 | 5 | 33 |
| [controlplane/udm/AbstractController.java](/controlplane/udm/AbstractController.java) | Java | 77 | 1 | 15 | 93 |
| [controlplane/udm/ControlPolicy.java](/controlplane/udm/ControlPolicy.java) | Java | 8 | 7 | 7 | 22 |
| [controlplane/udm/DummyController.java](/controlplane/udm/DummyController.java) | Java | 402 | 58 | 112 | 572 |
| [controlplane/udm/PerformanceEvaluator.java](/controlplane/udm/PerformanceEvaluator.java) | Java | 350 | 50 | 47 | 447 |
| [controlplane/udm/TestingController.java](/controlplane/udm/TestingController.java) | Java | 346 | 61 | 104 | 511 |
| [controlplane/udm/TestingWorkload.java](/controlplane/udm/TestingWorkload.java) | Java | 157 | 17 | 38 | 212 |
| [controlplane/webmonitor/StreamManagerRestfulGateway.java](/controlplane/webmonitor/StreamManagerRestfulGateway.java) | Java | 16 | 37 | 6 | 59 |
| [controlplane/webmonitor/StreamManagerWebMonitorEndpoint.java](/controlplane/webmonitor/StreamManagerWebMonitorEndpoint.java) | Java | 163 | 26 | 39 | 228 |

[summary](results.md)