class SQLQueries:
    create_table_stg01_nytimes_world = """
    CREATE TABLE IF NOT EXISTS 
    public.stg01_nytimes_world
    (slug_name VARCHAR
    ,section VARCHAR
    ,subsection VARCHAR
    ,title VARCHAR
    ,abstract VARCHAR
    ,uri VARCHAR
    ,url VARCHAR
    ,byline VARCHAR
    ,item_type VARCHAR
    ,source VARCHAR
    ,updated_date TIMESTAMP
    ,created_date TIMESTAMP
    ,published_date TIMESTAMP
    ,first_published_date TIMESTAMP
    ,material_type_facet VARCHAR);"""

    create_table_mio01_nytimes_world = """
    CREATE TABLE IF NOT EXISTS 
    public.mio01_nytimes_world
    (slug_name VARCHAR
    ,section VARCHAR
    ,subsection VARCHAR
    ,title VARCHAR
    ,abstract VARCHAR
    ,uri VARCHAR
    ,url VARCHAR
    ,byline VARCHAR
    ,item_type VARCHAR
    ,source VARCHAR
    ,updated_date TIMESTAMP
    ,created_date TIMESTAMP
    ,published_date TIMESTAMP
    ,first_published_date TIMESTAMP
    ,material_type_facet VARCHAR);"""

    upsert_query = """
        INSERT INTO public.mio01_nytimes_world
            (	slug_name 
                ,section 
                ,subsection 
                ,title 
                ,abstract 
                ,uri 
                ,url 
                ,byline 
                ,item_type 
                ,source 
                ,updated_date 
                ,created_date 
                ,published_date 
                ,first_published_date 
                ,material_type_facet 
            )
            SELECT *
            FROM (
                SELECT DISTINCT 
                slug_name 
                ,section 
                ,subsection 
                ,title 
                ,abstract 
                ,uri 
                ,url 
                ,byline 
                ,item_type 
                ,source 
                ,updated_date 
                ,created_date 
                ,published_date 
                ,first_published_date 
                ,material_type_facet
                FROM stg01_nytimes_world
            ) t
            ON CONFLICT ("uri") DO UPDATE
            SET
                slug_name = excluded.slug_name
                ,section = excluded.section
                ,subsection = excluded.subsection
                ,title = excluded.title
                ,abstract = excluded.abstract
                ,url = excluded.url
                ,byline = excluded.byline
                ,item_type = excluded.item_type
                ,source = excluded.source
                ,updated_date = excluded.updated_date
                ,created_date = excluded.created_date
                ,published_date = excluded.published_date
                ,first_published_date = excluded.first_published_date
                ,material_type_facet = excluded.material_type_facet;""" 
    
    delete_stg_table_query = """
        DROP TABLE IF EXISTS stg01_nytimes_world;
        """