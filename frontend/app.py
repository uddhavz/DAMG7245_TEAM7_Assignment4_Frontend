import streamlit as st
import re
import warnings
import ast
from snowflake.snowpark.exceptions import SnowparkSQLException
from langchain.llms import OpenAI
from langchain.utilities import SQLDatabase
from langchain_experimental.sql import SQLDatabaseChain
from langchain.chat_models import ChatOpenAI
from langchain.chains import create_sql_query_chain
from utils.snowchat_ui import StreamlitUICallbackHandler, message_func


st.set_page_config(page_title='ChatBoT', page_icon='🍥',layout = 'wide', initial_sidebar_state='collapsed')

warnings.filterwarnings("ignore")

INITIAL_MESSAGE = [
    {"role": "user", "content": "Hi!"},
    {
        "role": "assistant",
        "content": "Hello! I am Snowflake ChatBot, your SQL-speaking and ready to query Snowflake to get answers! ❄️🔍",
    },
]

# Initialize session state
if "messages" not in st.session_state:
    st.session_state["messages"] = INITIAL_MESSAGE
if "history" not in st.session_state:
    st.session_state["history"] = []
if "result" not in st.session_state:
    st.session_state["result"] = ""
if "prompt" not in st.session_state:
    st.session_state["prompt"] = ""

st.title("❄️ Snowflake SQL ChatBot ❄️")

# Prompt for user input and save

callback_handler = StreamlitUICallbackHandler()

if "snowflake_conn" not in st.session_state:
    st.session_state.snowflake_conn = st.connection("snowflake")


# Snowflake connection details
snowflake_url = f"snowflake://{st.secrets.user}:{st.secrets.password}@{st.secrets.account}/{st.secrets.database}/{st.secrets.schema}?warehouse={st.secrets.warehouse}&role={st.secrets.role}"

# Create SQLDatabase instance
if "conn" not in st.session_state:
    st.session_state["conn"] = SQLDatabase.from_uri(snowflake_url,sample_rows_in_table_info=3,schema=st.secrets.schema, include_tables=st.secrets.schema_artifacts, view_support=True)

st.success("Connected to Snowflake!")

chain = create_sql_query_chain(ChatOpenAI(temperature=0), st.session_state.conn)


def append_chat_history(question, answer):
    st.session_state["history"].append((question, answer))


def get_sql(text):
    sql_match = re.search(r"```sql\n(.*)\n```", text, re.DOTALL)
    return sql_match.group(1) if sql_match else None


def append_message(content, role="assistant", display=False):
    if role != "data":
        append_chat_history(st.session_state.messages[-2]["content"], content)
        return

    message = {"role": role, "content": content}
    st.session_state.messages.append(message)

    if callback_handler.has_streaming_ended:
        callback_handler.has_streaming_ended = False
        return


def handle_sql_exception(query, e, retries=2):
    append_message("Uh oh, I made an error, let me try to fix it..")
    error_message = (
        "You gave me a wrong SQL. FIX The SQL query by searching the schema definition:  \n```sql\n"
        + query
        + "\n```\n Error message: \n "
        + str(e)
    )
    new_query = chain({"question": error_message})  # , "chat_history": ""})["answer"]
    append_message(new_query)
    if get_sql(new_query) and retries > 0:
        return execute_sql(get_sql(new_query), st.session_state.conn, retries - 1)
    else:
        append_message("I'm sorry, I couldn't fix the error. Please try again.")
        st.stop()
        return None


def execute_sql(result, retries=2):
    if re.match(r"^\s*(drop|alter|truncate|delete|insert|update)\s", result, re.I):
        append_message("Sorry, I can't execute queries that can modify the database.")
        return None
    try:
        return st.session_state.snowflake_conn.query(result)
        # return st.session_state.conn.run(result)
    except Exception as e:
        return handle_sql_exception(result, e, retries)


st.header("ChatBot")


print(f'\n\n--------------INITIAL DISPLAY----------------')
print(st.session_state["messages"])
check_str = "$RUN"


for message in st.session_state.messages:
    message_func(
        message["content"],
        True if message["role"] == "user" else False,
        True if message["role"] == "data" else False,
    )

if prompt := st.chat_input():
    st.session_state.messages.append({"role": "user", "content": prompt})
    st.session_state.prompt= prompt
    print("\n\n-----------\n\n",st.session_state["messages"])
    print("\nprompt appended :",st.session_state["messages"][-1])
    message_func(prompt,True, False)

    if(st.session_state["prompt"][:4]!=check_str):
        # ------------- LANGCHAIN ----------
        if st.session_state["messages"] and st.session_state.get("messages") and st.session_state["messages"][-1]["role"] != "assistant":
            content = st.session_state["messages"][-1]["content"]    
            with st.chat_message("assistant"):
                result = chain.invoke({"question": content})
                st.markdown(f"**Generated SQL Query:**\n```sql\n{result}\n```")
                st.session_state.result = result
            # Append the result only if it hasn't been appended before
                st.session_state.messages.append({"role": "assistant", "content": result})
                print("\nresult appended :",st.session_state["messages"][-1])           
    else:
        # -------------PROMPT based RUN QUERY--------------
        sql_query = st.session_state["prompt"][4:]
        response = execute_sql(sql_query)
        if response is not None:
            
            st.session_state.messages.append({"role": "data", "content": response})
            print("\ndata appended :",st.session_state["messages"][-1])
            message_func(response,False, True)


    # ---- BUTTON EXECUTION ------    
if st.session_state.result is not None and st.button("Run Previous Response"):
    print("\n** Query Button Clicked **")

    if st.session_state.result == "":
        st.warning("Please ask a question")
        st.stop()

    # Execute the query only if the button has been clicked
    response = execute_sql(st.session_state.result)
    print(f"\nrecevied response : {type(response)}")
    if response is not None:
        
        st.session_state.messages.append({"role": "data", "content": response})
        print("\ndata appended :",st.session_state["messages"][-1])
        message_func(response,False, True)

        # Add a reset button
if "messages" in st.session_state.keys() and st.session_state["messages"] != INITIAL_MESSAGE:
    if st.button("Reset Chat"):
        del st.session_state["messages"]
        st.session_state["messages"] = INITIAL_MESSAGE
