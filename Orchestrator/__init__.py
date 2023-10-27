import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):
    yield context.call_activity("CopyBlobs", context.get_input())
    return "Success"


main = df.Orchestrator.create(orchestrator_function)
