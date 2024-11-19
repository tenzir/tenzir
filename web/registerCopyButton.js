import ExecutionEnvironment from "@docusaurus/ExecutionEnvironment";
import { registerCopyButton } from "@rehype-pretty/transformers";

export function onRouteDidUpdate() {
  if (ExecutionEnvironment.canUseDOM) {
    registerCopyButton();
  }
}
