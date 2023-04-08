package controllers

import Rate.Rate.getDataForApi
import db_controller.DbController._


import javax.inject._
import play.api.libs.json._
import play.api.mvc.{Action, AnyContent, BaseController, ControllerComponents}

@Singleton
class TodoListController @Inject()(val controllerComponents: ControllerComponents)
  extends BaseController {

  def writeDataIntoDb(period: String): Action[AnyContent] = Action {
    val Array(startDate, endDate) = period.split("to")
    val resp = writeRatesIntoDb(startDate, endDate)
    Ok("Данные загружены в бд")
  }

  def getDataForCurrency(period: String): Action[AnyContent] = Action {
    val Array(startDate, endDate) = period.split("to")
   val data =  getDataForApi(startDate, endDate)
    if (data.isEmpty) {
      NoContent
    } else {
      Ok(data)
    }
  }
}

