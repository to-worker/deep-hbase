package cn.cstor.face.meta

/**
  * Created on 2017/1/6
  *
  * @author feng.wei
  */
case class DataTables(namespace: String) extends Serializable {

    object FaceInfo extends Serializable {
        val name = s"$namespace:id_code"
        // 头像地址，姓名，性别，名族，出生年月，身份证号，详细信息（例如禁止出境等备注信息）
        val detail = List(
            "id"
            , "name"
            , "code"
            , "sex"
            , "img_addr"
            , "nation"
            , "mark", "code"
            , "sex"
            , "img_addr"
            , "nation"
            , "mark"
            , "birthday"
        )
    }

    object KeyPerson extends Serializable {
        val name = s"$namespace:key_person_info"
        // 身份证号,备注信息
        val detail = List("id", "message")
    }

    object Nation extends Serializable {
        val name = s"$namespace:nation"
        //  民族,民族编号
        val detail = List("nation","num")
    }

    object PerNation extends Serializable {
        val name = s"$namespace:person_nation"
        val detail = List("id","num")
    }

}
