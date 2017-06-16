# Copyright 2017 Renato Utsch
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

package(default_visibility = ["//visibility:private"])

py_binary(
    name = "receiver",
    srcs = ["receiver.py"],
    default_python_version = "PY3",
    srcs_version = "PY3",
)

py_binary(
    name = "sender",
    srcs = ["sender.py"],
    default_python_version = "PY3",
    srcs_version = "PY3",
)

py_binary(
    name = "server",
    srcs = ["server.py"],
    default_python_version = "PY3",
    srcs_version = "PY3",
)

py_library(
    name = "message",
    srcs = ["message.py"],
    srcs_version = "PY3",
)

py_test(
    name = "message_test",
    size = "small",
    srcs = ["message_test.py"],
    default_python_version = "PY3",
    srcs_version = "PY3",
    deps = [
        ":message",
        ":packet_manager",
    ],
)
