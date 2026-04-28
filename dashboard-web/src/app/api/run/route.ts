import { NextResponse } from "next/server";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import { appConfig } from "@/lib/config";

export const dynamic = "force-dynamic";

export async function POST(req: Request) {
  try {
    const body = await req.json();
    const pipelineName = body?.pipeline_name;
    if (!pipelineName) {
      return NextResponse.json({ error: "pipeline_name is required" }, { status: 400 });
    }

    const lambda = new LambdaClient({ region: appConfig.region });
    const result = await lambda.send(
      new InvokeCommand({
        FunctionName: appConfig.lambdaName,
        InvocationType: "RequestResponse",
        Payload: Buffer.from(JSON.stringify({ pipeline_name: pipelineName })),
      })
    );
    const payload = JSON.parse(Buffer.from(result.Payload || []).toString("utf8") || "{}");
    return NextResponse.json({ status: "ok", lambda_response: payload });
  } catch (error) {
    return NextResponse.json(
      { status: "error", message: error instanceof Error ? error.message : "Unknown error" },
      { status: 500 }
    );
  }
}
