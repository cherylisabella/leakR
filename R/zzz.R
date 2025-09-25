#' @keywords internal
#' @keywords internal
#' @name ____TEMP_FIX
#' @title FIX ME
#' @param libname TODO: Document
#' @param pkgname TODO: Document
#' @keywords internal
#' @keywords internal
.onLoad <- function(libname, pkgname) {
  ns <- asNamespace(pkgname)
  detector_funs <- ls(envir = ns, pattern = "^detect_")
  for (fun_name in detector_funs) {
    fun <- get(fun_name, envir = ns)
    detector_name <- sub("^detect_", "", fun_name)
    desc <- paste("Auto-registered detector", detector_name)
    register_detector(detector_name, fun, desc)
  }
}
